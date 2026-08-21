# TCP Gateway

A Spring Boot service that bridges HTTP/REST callers to a bank/payment-scheme
counterparty over persistent, framed TCP connections carrying ISO 8583 (FPS-style)
messages — in both directions.

- **Outbound (this service → bank):** an HTTP caller POSTs a payment/network-management
  request; the gateway sends it over TCP, correlates the reply, and returns it as the
  HTTP response.
- **Inbound (bank → this service):** the bank pushes a request on a connection this
  service opened; the gateway builds and sends the correct reply automatically, on the
  exact same connection.

## Architecture

### Wire protocol

Every frame, in both directions, looks like:

```
[4 bytes: length, big-endian][ISO 8583 body]
```

The 4-byte length prefix covers just the body — nothing proprietary rides alongside it.
A `LengthFieldBasedFrameDecoder`/`LengthFieldPrepender` pair handles framing on
read/write, so application code only ever deals with the raw ISO 8583 body.

There is no correlation ID (or any other header) on the wire in either direction.
Requests and responses are matched purely by the Transaction Reference Number (TRN)
already carried inside the ISO 8583 body (DE31) — the same field a human would use to
trace a payment. A short-lived, internal-only correlation ID is generated per outbound
dispatch purely so log lines can be grepped end-to-end (connection id + correlation id +
TRN); it never goes over the wire and plays no part in matching.

### Connection pools

`TcpConnectionPoolManager` opens one `TcpConnectionPool` per configured PPG (payment
processing gateway) entry — each pool holds N outbound TCP connections (this service is
always the one that dials out, `pool-size` per pool is configurable). Every connection
gets its own persistent read loop; correlated replies are matched via the TRN parsed
out of the response body and a `CorrelationRegistry`.

Pool names are a `PaymentTypes` enum value, and direction is named from the perspective
of who **initiates the payment request**, not who dials the TCP connection (both
directions run over connections this service opens):

| Pool      | Direction | Meaning                                                              |
|-----------|-----------|-----------------------------------------------------------------------|
| `SIP_IN`  | inbound   | this service sends Single Immediate Payment requests to the bank      |
| `SOP_IN`  | inbound   | this service sends Standing Order requests to the bank                |
| `OTH_IN`  | inbound   | this service sends other payment types (bulk auth, forward-dated, return, scheme return, corporate bulk) to the bank |
| `USM_IN`  | inbound   | this service sends network management requests (sign-on, echo-test, etc.) to the bank; also the channel a bank-initiated 9804/9624 push lands on |
| `SIP_OUT` | outbound  | the bank pushes Single Immediate Payment requests to this service, which auto-replies |
| `SOP_OUT` | outbound  | the bank pushes Standing Order requests to this service                |
| `OTH_OUT` | outbound  | the bank pushes other payment types to this service                    |

There is no `USM_OUT` — network management is inbound-only.

### Inbound (bank-initiated) message handling

Every connection's read loop first checks the inbound frame's MTI:

- **Response-shaped MTI** (`9210`, `9430`, `9814`, `9834`) — this service never sends
  these MTIs itself, so an inbound one is always the reply to something it sent. Its
  TRN is parsed out of the body and matched against the `CorrelationRegistry`; a match
  unblocks the waiting HTTP caller (or `tcp-stub-tester`-style test caller) with the
  response bytes. No match means a late/stray response — logged and routed to
  `UnsolicitedMessageHandler`.
- **Any other MTI** — necessarily bank-initiated. It's dispatched by MTI:

| MTI (unsolicited) | Handling |
|---|---|
| `9804` (network management request) | build a `9814` approval carrying the same TRN/function code, send back on the **same connection** |
| `9200` (payment request from bank) | build a `9210` acceptance carrying the same TRN, send back the same way |
| `9624` (admin advice) | logged at WARN (functionCode, infoText) and dropped — no TRN, no reply |
| anything else | routed to `UnsolicitedMessageHandler` (log-only fallback) |

A reply to an unsolicited push is always sent on the exact connection object the
request arrived on (never round-robined to a different connection in the pool), so
replies can't cross onto the wrong socket.

### Debug logging

Set `logging.level.com.jackbourner.tcpgateway: DEBUG` to log every request/response
model involved in the flows above, both as JSON and as the raw ISO 8583 wire string —
useful for tracing exactly what went out and what came back for a given corrId/TRN.

## Configuration

`tcp-gateway.pools` in `application.yml`, one entry per `PaymentTypes` value:

```yaml
tcp-gateway:
  response-timeout-ms: 30000
  pools:
    SIP_IN:
      host: localhost
      port: 9001
      pool-size: 5
    SOP_IN:
      host: localhost
      port: 9002
      pool-size: 3
    # ... one block per pool
```

## REST API

### Payment (typed JSON)

| Method | Path | Body | Notes |
|---|---|---|---|
| POST | `/{direction}/{ppg}/iso8583/payment` | `PaymentRequestFromCIToBank_920x` | send a real 9200, get the 9210 back |
| POST | `/inbound/{ppg}/iso8583/generatePayment` | — | builds and sends a synthetic 9200 test payment |

### Network management triggers (9804 → 9814)

No request body — each builds a `NetworkManagementRequest_9804` with the matching
function code and dispatches it (typically through `USM_IN`):

| Path | Function code |
|---|---|
| POST `/{ppg}/iso8583/sign-on` | 881 — sign-on to submit |
| POST `/{ppg}/iso8583/sign-on-receive` | 891 — sign-on to receive |
| POST `/{ppg}/iso8583/sign-off` | 882 — sign-off to submit |
| POST `/{ppg}/iso8583/sign-off-receive` | 892 — sign-off to receive |
| POST `/{ppg}/iso8583/key-change` | 811 — key change |
| POST `/{ppg}/iso8583/new-key-request` | 885 — new key request |
| POST `/{ppg}/iso8583/echo-test` | 831 — echo test |
| POST `/{ppg}/iso8583/key-verification` | 886 — key verification |

### Payment repeat and reversal triggers

No request body — builds a synthetic test payment/reversal:

| Path | MTI | Notes |
|---|---|---|
| POST `/{ppg}/iso8583/repeat` | 9201 | payment repeat request |
| POST `/{ppg}/iso8583/reversal` | 9420 | payment reversal request, expects 9430 |
| POST `/{ppg}/iso8583/reversal-repeat` | 9421 | reversal repeat request |

### Other payment-type triggers

Same synthetic 9200 test payment as `generatePayment`, with just the processing-code
payment-type digits overridden — a simplified trigger, not a fully populated real
message (e.g. a real return would also carry `returnedPaymentFpid`):

| Path | Processing code type |
|---|---|
| POST `/{ppg}/iso8583/return` | 20 — return payment |
| POST `/{ppg}/iso8583/scheme-return` | 25 — scheme return payment |
| POST `/{ppg}/iso8583/bulk-auth` | 00 — bulk authorisation |
| POST `/{ppg}/iso8583/forward-dated` | 40 — forward-dated payment |
| POST `/{ppg}/iso8583/corporate-bulk` | 50 — corporate bulk payment |

> All of the trigger endpoints above use `/{ppg}/iso8583/{action}` (no `{direction}`
> segment, since the direction is implied by the action) — this is a different
> convention from the older `/inbound/{ppg}/iso8583/generatePayment`, kept as-is rather
> than silently renamed.

### Raw pass-through

For sending pre-built ISO 8583 bytes directly, bypassing JSON mapping entirely:

| Method | Path | Body |
|---|---|---|
| POST | `/{direction}/{ppg}/ci-to-bank` | raw `application/octet-stream` |
| POST | `/{direction}/{ppg}/` | raw `application/octet-stream` |

## Build & run

```
mvn spring-boot:run   # start the service
mvn verify             # run tests
```

## Related projects

- **`iso8583-library`** — the shared ISO 8583/FPS message models, mappers, and codec
  this service and its test tooling both depend on.
- **`stub/`** (this repo) — an unused, unmaintained prototype TCP stub (not wired into
  the Maven build, last touched in the initial commit, depends on the older `j8583`
  library rather than `iso8583-library`). It predates and was superseded by
  `tcp-stub-tester` below, and still speaks the old corrId-prefixed wire protocol — it
  is **not** interoperable with this gateway.
- **`tcp-stub-tester`** — a separate TCP stub that plays the bank/scheme role for
  testing the gateway's inbound (`_OUT`) flows, including pushing 9200/9804 requests
  and waiting for correlated replies via `/stub/{ppg}/send`.
