package com.jackbourner.tcpgateway.pool;

import com.jackbourner.iso8583.codec.Mappers;
import com.jackbourner.iso8583.codec.MessageParser;
import com.jackbourner.iso8583.models.AdminAdvice_9624;
import com.jackbourner.iso8583.models.FpsMessage;
import com.jackbourner.iso8583.models.PaymentTypes;
import com.jackbourner.iso8583.protocol.MessageSchema;
import tools.jackson.databind.ObjectMapper;
import com.jackbourner.tcpgateway.config.PoolConfig;
import com.jackbourner.tcpgateway.correlation.CorrelationRegistry;
import com.jackbourner.tcpgateway.handler.UnsolicitedMessageHandler;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.tcp.TcpClient;

import java.time.Duration;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static com.jackbourner.iso8583.protocol.Constants.ADMINISTRATION_ADVICE_9624;
import static com.jackbourner.iso8583.protocol.Constants.NETWORK_MESSAGE_9804;
import static com.jackbourner.iso8583.protocol.Constants.NETWORK_MESSAGE_9814;
import static com.jackbourner.iso8583.protocol.Constants.PAYMENT_REQUEST_9200;
import static com.jackbourner.iso8583.protocol.Constants.PAYMENT_RESPONSE_9210;
import static com.jackbourner.iso8583.protocol.Constants.PAYMENT_REVERSAL_RESPONSE_9430;
import static com.jackbourner.iso8583.protocol.Constants.UNSOLICITED_MESSAGE_RESPONSE_9834;
import static com.jackbourner.iso8583.protocol.Constants.WINDOWS_1252;

/**
 * Manages N persistent outbound TCP connections to a single host:port.
 *
 * <h3>Wire frame format</h3>
 * <pre>
 *   [4 bytes: payload length (big-endian)] [N bytes: ISO 8583 body]
 * </pre>
 * The 4-byte length prefix covers only the body. {@link LengthFieldBasedFrameDecoder}
 * strips the prefix so the read loop receives just the body. No correlation ID is
 * carried on the wire in either direction — only the TRN, which lives inside the body.
 *
 * <h3>Routing</h3>
 * Inbound frames are routed by MTI (first 4 bytes of the body):
 * <ul>
 *   <li>A <b>response-shaped</b> MTI ({@code 9210}, {@code 9430}, {@code 9814},
 *       {@code 9834}) is always the reply to something this service sent. Its TRN is
 *       parsed out and matched against {@link CorrelationRegistry#complete} to wake the
 *       blocked caller; no match means a late/stray response, which is logged and routed
 *       to {@link UnsolicitedMessageHandler}.</li>
 *   <li>Any other MTI is necessarily server-initiated (this service never expects an echo
 *       of its own request MTI back as a reply):
 *       <ul>
 *         <li>{@code 9804} (network management request) → a {@code 9814} approval is
 *             built and sent straight back on the <b>same</b> connection.</li>
 *         <li>{@code 9624} (admin advice) → logged and dropped; no TRN, no reply.</li>
 *         <li>{@code 9200} (payment request from bank) → a {@code 9210} acceptance is
 *             built and sent back the same way.</li>
 *         <li>anything else → {@link UnsolicitedMessageHandler#handle}.</li>
 *       </ul>
 *   </li>
 * </ul>
 * A reply to a server-initiated push is always sent back on the exact connection object
 * the request arrived on (never round-robined), so replies can't cross onto another socket.
 */

@Log4j2
public class TcpConnectionPool {

    /** Maximum frame size accepted from the server (bytes). */
    private static final int MAX_FRAME_SIZE = 4 * 1024 * 1024; // 4 MB

    private static final Duration RECONNECT_DELAY = Duration.ofSeconds(5);

    /** Used only to render request/response models as JSON for debug logging. */
    private static final ObjectMapper JSON = new ObjectMapper();

    @Getter
    private final PaymentTypes name;
    private final String host;
    private final int port;
    private final int poolSize;
    private final CorrelationRegistry correlationRegistry;
    private final UnsolicitedMessageHandler unsolicitedHandler;

    /** Live connections; thread-safe via CopyOnWriteArrayList. */
    private final CopyOnWriteArrayList<Connection> connections = new CopyOnWriteArrayList<>();

    /** Round-robin cursor; mod taken against live connection count on each use. */
    private final AtomicInteger cursor = new AtomicInteger(0);

    /** Set to true when the pool is being shut down so reconnect loops stop. */
    private volatile boolean closing = false;

    TcpConnectionPool(PaymentTypes name, PoolConfig config,
                      CorrelationRegistry correlationRegistry,
                      UnsolicitedMessageHandler unsolicitedHandler) {
        this.name = name;
        this.host = config.getHost();
        this.port = config.getPort();
        this.poolSize = config.getPoolSize();
        this.correlationRegistry = correlationRegistry;
        this.unsolicitedHandler = unsolicitedHandler;
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    /** Opens all N connections. Called once by {@link TcpConnectionPoolManager} at startup. */
    void init() {
        log.info("Initialising TCP pool '{}' → {}:{} ({} connections)", name, host, port, poolSize);
        for (int i = 0; i < poolSize; i++) {
            openConnection();
        }
    }

    /** Drains all connections gracefully. */
    public void close() {
        closing = true;
        connections.forEach(Connection::dispose);
        connections.clear();
        log.info("TCP pool '{}' closed", name);
    }

    // -------------------------------------------------------------------------
    // Connection management
    // -------------------------------------------------------------------------

    private void openConnection() {
        TcpClient.create()
                .host(host)
                .port(port)
                .doOnConnected(conn -> {
                    /*
                     * Add a length-field frame decoder so each onNext in the read loop
                     * is exactly one complete frame:
                     *   maxFrameLength  = MAX_FRAME_SIZE
                     *   lengthFieldOffset = 0
                     *   lengthFieldLength = 4
                     *   lengthAdjustment  = 0
                     *   initialBytesToStrip = 4  ← strips the length prefix
                     */
                    conn.addHandlerLast("frameDecoder",
                            new LengthFieldBasedFrameDecoder(MAX_FRAME_SIZE, 0, 4, 0, 4));
                    /*
                     * LengthFieldPrepender automatically prepends a 4-byte length header
                     * on the outbound side, so send() only needs to write the raw body.
                     */
                    conn.addHandlerFirst("framePrepender",
                            new LengthFieldPrepender(4));
                })
                .connect()
                .subscribe(
                        this::onConnected,
                        err -> {
                            log.warn("Pool '{}' connect failed: {} — retrying in {}",
                                    name, err.getMessage(), RECONNECT_DELAY);
                            scheduleReconnect();
                        });
    }

    private void onConnected(Connection conn) {
        log.info("Pool '{}' connection established → {}:{}", name, host, port);
        connections.add(conn);

        conn.inbound()
                .receive()
                .retain()          // keep ByteBuf alive past the Netty pipeline
                .asByteArray()
                .doOnNext(frame -> routeInbound(conn, frame))
                .doOnError(err -> log.error("Pool '{}' read error: {}", name, err.getMessage()))
                .doFinally(signal -> {
                    connections.remove(conn);
                    log.warn("Pool '{}' connection closed (signal={})", name, signal);
                    if (!closing) {
                        scheduleReconnect();
                    }
                })
                .subscribe();
    }

    private void scheduleReconnect() {
        if (closing) return;
        Mono.delay(RECONNECT_DELAY)
                .subscribe(__ -> openConnection());
    }

    // -------------------------------------------------------------------------
    // Inbound routing
    // -------------------------------------------------------------------------

    /** Called for every inbound frame (after the 4-byte length prefix is stripped). */
    private void routeInbound(Connection conn, byte[] body) {
        if (body.length < 4) {
            log.warn("Pool '{}' connId={} received undersized frame ({} bytes) — discarding",
                    name, connId(conn), body.length);
            return;
        }

        String mti;
        try {
            mti = MessageParser.extractMessageType(body);
        } catch (Exception e) {
            log.warn("Pool '{}' connId={} — could not read MTI ({}), routing to unsolicited handler",
                    name, connId(conn), e.getMessage());
            unsolicitedHandler.handle(name, null, body);
            return;
        }

        if (isResponseMti(mti)) {
            routeResponse(conn, mti, body);
        } else {
            handleUnsolicited(conn, mti, body);
        }
    }

    /**
     * Response-shaped MTIs are always the reply to something this service sent — the
     * gateway never sends these MTIs itself, and the bank never pushes them unprompted.
     */
    private static boolean isResponseMti(String mti) {
        return switch (mti) {
            case PAYMENT_RESPONSE_9210, PAYMENT_REVERSAL_RESPONSE_9430,
                 NETWORK_MESSAGE_9814, UNSOLICITED_MESSAGE_RESPONSE_9834 -> true;
            default -> false;
        };
    }

    /** Parses the TRN out of a response-shaped frame and wakes the matching waiter. */
    private void routeResponse(Connection conn, String mti, byte[] body) {
        String trn;
        try {
            trn = extractResponseTrn(mti, body);
        } catch (Exception e) {
            log.warn("Pool '{}' connId={} mti={} — failed to parse response body for TRN ({}), discarding",
                    name, connId(conn), mti, e.getMessage());
            return;
        }

        if (!correlationRegistry.complete(trn, body)) {
            log.warn("Pool '{}' connId={} mti={} trn='{}' — no pending request for this TRN, routing to unsolicited handler",
                    name, connId(conn), mti, trn);
            unsolicitedHandler.handle(name, trn, body);
        }
    }

    /** Parses a response-shaped MTI (bank-to-CI direction) into its typed model to read the TRN. */
    private static String extractResponseTrn(String mti, byte[] body) {
        FpsMessage msg = switch (mti) {
            case PAYMENT_RESPONSE_9210 -> Mappers.PAYMENT_RESPONSE_BANK_TO_CI_MAPPER.fromBytes(body);
            case PAYMENT_REVERSAL_RESPONSE_9430 -> Mappers.PAYMENT_REVERSAL_RESPONSE_MAPPER.fromBytes(body);
            case NETWORK_MESSAGE_9814 -> Mappers.NETWORK_MANAGEMENT_RESPONSE_MAPPER.fromBytes(body);
            case UNSOLICITED_MESSAGE_RESPONSE_9834 -> Mappers.UNSOLICITED_RESPONSE_MAPPER.fromBytes(body);
            default -> throw new IllegalArgumentException("Unexpected response MTI: " + mti);
        };
        return msg.getTransactionReferenceNumber();
    }

    // -------------------------------------------------------------------------
    // Unsolicited (server-initiated) frames
    // -------------------------------------------------------------------------

    /** Handles a frame whose MTI is not response-shaped — i.e. server-initiated. */
    private void handleUnsolicited(Connection conn, String mti, byte[] body) {
        switch (mti) {
            case NETWORK_MESSAGE_9804 -> handle9804(conn, body);
            case ADMINISTRATION_ADVICE_9624 -> handle9624(conn, body);
            case PAYMENT_REQUEST_9200 -> handle9200FromBank(conn, body);
            default -> unsolicitedHandler.handle(name, extractTrnQuietly(body), body);
        }
    }

    /** Best-effort TRN extraction for logging an otherwise-unrecognised MTI; never throws. */
    private static String extractTrnQuietly(byte[] body) {
        try {
            return MessageParser.parseToModel(body).getTransactionReferenceNumber();
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * MTI 9804 (network management request): build a 9814 approval carrying the same
     * TRN and function code, and send it back on the exact connection the request
     * arrived on, so replies never cross onto another socket.
     */
    private void handle9804(Connection conn, byte[] body) {
        handlePushAndReply(conn, body, "9804", "9814",
                Mappers.NETWORK_MANAGEMENT_REQUEST_MAPPER::fromBytes,
                Mappers.NETWORK_MANAGEMENT_RESPONSE_MAPPER::fromRequest,
                "9804 received, replying 9814 (approved)");
    }

    /**
     * MTI 9200 that isn't response-shaped — necessarily a payment request pushed by
     * the bank (this service's own outbound 9200 always gets an inbound 9210 back,
     * never an inbound 9200). Builds a 9210 acceptance carrying the same TRN and sends
     * it back on the exact connection it arrived on.
     */
    private void handle9200FromBank(Connection conn, byte[] body) {
        handlePushAndReply(conn, body, "9200", "9210",
                b -> Mappers.PAYMENT_REQUEST_BANK_TO_CI_MAPPER.fromMessage(
                        MessageSchema.PAYMENT_REQUEST_BANK_TO_CI.parse(b)),
                Mappers.PAYMENT_RESPONSE_CI_TO_BANK_MAPPER::fromRequest,
                "9200 payment request received from bank, replying 9210 (accepted)");
    }

    /**
     * Shared shape for every "unsolicited request in, auto-reply out" flow: parse the
     * request, debug-log it, build the reply via {@code buildResponse}, log the summary
     * and the reply, then send the reply back pinned to {@code conn}.
     * A parse failure is logged and swallowed — no reply is sent.
     */
    private <REQ extends FpsMessage, RESP extends FpsMessage> void handlePushAndReply(
            Connection conn, byte[] body, String requestMti, String responseMti,
            Function<byte[], REQ> parse, Function<REQ, RESP> buildResponse, String summary) {
        REQ request;
        try {
            request = parse.apply(body);
        } catch (Exception e) {
            log.error("Pool '{}' connId={} — failed to parse {} request: {}",
                    name, connId(conn), requestMti, e.getMessage());
            return;
        }
        logMessage("request " + requestMti, conn, request, body);

        String trn = request.getTransactionReferenceNumber();
        RESP response = buildResponse.apply(request);
        log.info("Pool '{}' connId={} trn='{}' — {}", name, connId(conn), trn, summary);

        byte[] responseBytes = response.toIsoBytes();
        logMessage("response " + responseMti, conn, response, responseBytes);

        sendOn(conn, trn, responseBytes)
                .doOnError(err -> log.error("Pool '{}' connId={} trn='{}' — failed to send {} reply: {}",
                        name, connId(conn), trn, responseMti, err.getMessage()))
                .subscribe();
    }

    /** MTI 9624 (admin advice): no TRN, no reply — logged and dropped. */
    private void handle9624(Connection conn, byte[] body) {
        try {
            AdminAdvice_9624 advice = Mappers.ADMIN_ADVICE_MAPPER.fromBytes(body);
            log.warn("Pool '{}' connId={} — 9624 admin advice ignored: functionCode='{}' ({}) infoText='{}'",
                    name, connId(conn), advice.getFunctionCode(), advice.getFunctionCodeDescription(), advice.getInfoText());
            logMessage("request 9624", conn, advice, body);
        } catch (Exception e) {
            log.warn("Pool '{}' connId={} — 9624 admin advice ignored (unparsable: {})",
                    name, connId(conn), e.getMessage());
        }
    }

    /** Stable short identifier for a connection, used to tie log lines to a specific socket. */
    private static String connId(Connection conn) {
        return conn.channel().id().asShortText();
    }

    /**
     * Debug-logs a request/response model in both JSON and raw ISO 8583 wire form.
     * {@code isoBytes} is the exact bytes received from (or sent to) the connection —
     * decoded as windows-1252, matching how the ISO 8583 library encodes messages.
     */
    private void logMessage(String label, Connection conn, FpsMessage msg, byte[] isoBytes) {
        if (!log.isDebugEnabled()) return;
        log.debug("Pool '{}' connId={} {} json={} iso8583='{}'",
                name, connId(conn), label, toJson(msg), new String(isoBytes, WINDOWS_1252));
    }

    private static String toJson(FpsMessage msg) {
        try {
            return JSON.writeValueAsString(msg);
        } catch (Exception e) {
            return "<json serialization failed: " + e.getMessage() + ">";
        }
    }

    // -------------------------------------------------------------------------
    // Outbound sending
    // -------------------------------------------------------------------------

    /**
     * Sends the raw ISO 8583 body on the next available connection (round-robin).
     * No correlation ID or other header is added — the wire carries exactly {@code body}.
     *
     * @param trn  Transaction Reference Number, for logging only (matching happens by
     *             parsing the TRN back out of the response body on receipt)
     * @param body raw ISO 8583 request payload
     * @return Mono that completes when the bytes are flushed to the OS buffer
     * @throws IllegalStateException if no connections are currently live
     */
    public Mono<Void> send(String trn, byte[] body) {
        return sendOn(nextConnection(), trn, body);
    }

    /**
     * Sends a message on a specific connection — used to reply to an unsolicited
     * push on the exact socket it arrived on, rather than round-robining.
     */
    private Mono<Void> sendOn(Connection conn, String trn, byte[] body) {
        log.debug("Pool '{}' connId={} trn='{}' — sending {} bytes",
                name, connId(conn), trn, body.length);
        /*
         * LengthFieldPrepender in the pipeline prepends the 4-byte length header
         * automatically, so we only pass the raw body here.
         */
        return conn.outbound()
                .sendByteArray(Mono.just(body))
                .then();
    }

    private Connection nextConnection() {
        if (connections.isEmpty()) {
            throw new IllegalStateException(
                    "TCP pool '" + name + "' has no live connections");
        }
        // Bit-mask keeps the cursor non-negative even after Integer overflow.
        int idx = (cursor.getAndIncrement() & Integer.MAX_VALUE) % connections.size();
        return connections.get(idx);
    }

    // -------------------------------------------------------------------------
    // Accessors
    // -------------------------------------------------------------------------

    public int liveConnectionCount() { return connections.size(); }
}
