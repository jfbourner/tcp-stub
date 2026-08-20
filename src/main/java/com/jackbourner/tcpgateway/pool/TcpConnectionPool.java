package com.jackbourner.tcpgateway.pool;

import com.jackbourner.iso8583.codec.Mappers;
import com.jackbourner.iso8583.models.AdminAdvice_9624;
import com.jackbourner.iso8583.models.FpsMessage;
import com.jackbourner.iso8583.models.PaymentTypes;
import com.jackbourner.iso8583.protocol.MessageParser;
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

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static com.jackbourner.iso8583.protocol.Constants.ADMINISTRATION_ADVICE_9624;
import static com.jackbourner.iso8583.protocol.Constants.NETWORK_MESSAGE_9804;
import static com.jackbourner.iso8583.protocol.Constants.PAYMENT_REQUEST_9200;
import static com.jackbourner.iso8583.protocol.Constants.WINDOWS_1252;

/**
 * Manages N persistent outbound TCP connections to a single host:port.
 *
 * <h3>Wire frame format</h3>
 * <pre>
 *   [4 bytes: payload length (big-endian)] [12 bytes: correlation ID (ASCII, space-padded)] [N bytes: body]
 * </pre>
 * The 4-byte length prefix covers only the bytes that follow it (corrId + body).
 * {@link LengthFieldBasedFrameDecoder} strips the prefix so the read loop receives
 * {@code [12-byte corrId][body]}.
 *
 * <h3>Routing</h3>
 * Inbound frames are routed by correlation ID:
 * <ul>
 *   <li>Matched ID → {@link CorrelationRegistry#complete} wakes the blocked caller.</li>
 *   <li>Unknown ID  → the connection pushed this frame unsolicited (server-initiated).
 *       It is inspected by MTI (first 4 bytes of the body):
 *       <ul>
 *         <li>{@code 9804} (network management request) → a {@code 9814} approval is
 *             built and sent straight back on the <b>same</b> connection, echoing the
 *             same corrId, so the reply can never cross onto a different socket.</li>
 *         <li>{@code 9624} (admin advice) → logged and dropped; no TRN, no reply.</li>
 *         <li>{@code 9200} (payment request from bank) → a {@code 9210} acceptance is
 *             built and sent back the same way. Any unmatched {@code 9200} is
 *             necessarily bank-initiated — a CI-initiated {@code 9200}'s {@code 9210}
 *             reply would already have matched a pending correlation ID above.</li>
 *         <li>anything else → {@link UnsolicitedMessageHandler#handle}.</li>
 *       </ul>
 *   </li>
 * </ul>
 */

@Log4j2
public class TcpConnectionPool {

    /** Length of the correlation-ID field in every frame header (bytes). */
    static final int CORR_ID_LENGTH = 12;

    /** Maximum frame size accepted from the server (bytes, inclusive of corrId). */
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
                     * on the outbound side, so send() only needs to write [corrId][body].
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

    /**
     * Called for every inbound frame (after the 4-byte length prefix is stripped).
     * Frame layout: {@code [CORR_ID_LENGTH bytes corrId][body]}
     */
    private void routeInbound(Connection conn, byte[] frame) {
        if (frame.length < CORR_ID_LENGTH) {
            log.warn("Pool '{}' received undersized frame ({} bytes) — discarding", name, frame.length);
            return;
        }

        String corrId = extractCorrId(frame);
        byte[] body = Arrays.copyOfRange(frame, CORR_ID_LENGTH, frame.length);

        if (!correlationRegistry.complete(corrId, body)) {
            handleUnsolicited(conn, corrId, body);
        }
    }

    private static String extractCorrId(byte[] frame) {
        return new String(frame, 0, CORR_ID_LENGTH, StandardCharsets.US_ASCII).trim();
    }

    // -------------------------------------------------------------------------
    // Unsolicited (server-initiated) frames
    // -------------------------------------------------------------------------

    /**
     * Handles a frame whose corrId matched no pending request — i.e. server-initiated.
     * Dispatches by MTI (first 4 bytes of the body).
     */
    private void handleUnsolicited(Connection conn, String corrId, byte[] body) {
        String mti;
        try {
            mti = MessageParser.extractMessageType(body);
        } catch (Exception e) {
            log.warn("Pool '{}' connId={} corrId='{}' — could not read MTI ({}), routing to unsolicited handler",
                    name, connId(conn), corrId, e.getMessage());
            unsolicitedHandler.handle(name, corrId, body);
            return;
        }

        switch (mti) {
            case NETWORK_MESSAGE_9804 -> handle9804(conn, corrId, body);
            case ADMINISTRATION_ADVICE_9624 -> handle9624(conn, corrId, body);
            case PAYMENT_REQUEST_9200 -> handle9200FromBank(conn, corrId, body);
            default -> unsolicitedHandler.handle(name, corrId, body);
        }
    }

    /**
     * MTI 9804 (network management request): build a 9814 approval carrying the same
     * TRN and function code, and send it back on the exact connection the request
     * arrived on, echoing the same corrId, so replies never cross onto another socket.
     */
    private void handle9804(Connection conn, String corrId, byte[] body) {
        handlePushAndReply(conn, corrId, body, "9804", "9814",
                Mappers.NETWORK_MANAGEMENT_REQUEST_MAPPER::fromBytes,
                Mappers.NETWORK_MANAGEMENT_RESPONSE_MAPPER::fromRequest,
                "9804 received, replying 9814 (approved)");
    }

    /**
     * MTI 9200 with no matching correlation — necessarily a payment request pushed by
     * the bank (a CI-initiated 9200's 9210 reply would already have matched a pending
     * correlation ID before we get here). Builds a 9210 acceptance carrying the same
     * TRN and sends it back on the exact connection it arrived on, echoing the corrId.
     */
    private void handle9200FromBank(Connection conn, String corrId, byte[] body) {
        handlePushAndReply(conn, corrId, body, "9200", "9210",
                b -> Mappers.PAYMENT_REQUEST_BANK_TO_CI_MAPPER.fromMessage(
                        MessageSchema.PAYMENT_REQUEST_BANK_TO_CI.parse(b)),
                Mappers.PAYMENT_RESPONSE_CI_TO_BANK_MAPPER::fromRequest,
                "9200 payment request received from bank, replying 9210 (accepted)");
    }

    /**
     * Shared shape for every "unsolicited request in, auto-reply out" flow: parse the
     * request, debug-log it, build the reply via {@code buildResponse}, log the summary
     * and the reply, then send the reply back pinned to {@code conn} with the same corrId.
     * A parse failure is logged and swallowed — no reply is sent.
     */
    private <REQ extends FpsMessage, RESP extends FpsMessage> void handlePushAndReply(
            Connection conn, String corrId, byte[] body, String requestMti, String responseMti,
            Function<byte[], REQ> parse, Function<REQ, RESP> buildResponse, String summary) {
        REQ request;
        try {
            request = parse.apply(body);
        } catch (Exception e) {
            log.error("Pool '{}' connId={} corrId='{}' — failed to parse {} request: {}",
                    name, connId(conn), corrId, requestMti, e.getMessage());
            return;
        }
        logMessage("request " + requestMti, conn, corrId, request, body);

        String trn = request.getTransactionReferenceNumber();
        RESP response = buildResponse.apply(request);
        log.info("Pool '{}' connId={} corrId='{}' trn='{}' — {}", name, connId(conn), corrId, trn, summary);

        byte[] responseBytes = response.toIsoBytes();
        logMessage("response " + responseMti, conn, corrId, response, responseBytes);

        sendOn(conn, corrId, responseBytes)
                .doOnError(err -> log.error("Pool '{}' connId={} corrId='{}' trn='{}' — failed to send {} reply: {}",
                        name, connId(conn), corrId, trn, responseMti, err.getMessage()))
                .subscribe();
    }

    /** MTI 9624 (admin advice): no TRN, no reply — logged and dropped. */
    private void handle9624(Connection conn, String corrId, byte[] body) {
        try {
            AdminAdvice_9624 advice = Mappers.ADMIN_ADVICE_MAPPER.fromBytes(body);
            log.warn("Pool '{}' connId={} corrId='{}' — 9624 admin advice ignored: functionCode='{}' ({}) infoText='{}'",
                    name, connId(conn), corrId, advice.getFunctionCode(), advice.getFunctionCodeDescription(), advice.getInfoText());
            logMessage("request 9624", conn, corrId, advice, body);
        } catch (Exception e) {
            log.warn("Pool '{}' connId={} corrId='{}' — 9624 admin advice ignored (unparsable: {})",
                    name, connId(conn), corrId, e.getMessage());
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
    private void logMessage(String label, Connection conn, String corrId, FpsMessage msg, byte[] isoBytes) {
        if (!log.isDebugEnabled()) return;
        log.debug("Pool '{}' connId={} corrId='{}' {} json={} iso8583='{}'",
                name, connId(conn), corrId, label, toJson(msg), new String(isoBytes, WINDOWS_1252));
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
     * Sends a framed request on the next available connection (round-robin).
     *
     * @param correlationId 1–12 ASCII characters; padded/truncated to {@value #CORR_ID_LENGTH} bytes
     * @param body          raw request payload
     * @return Mono that completes when the bytes are flushed to the OS buffer
     * @throws IllegalStateException if no connections are currently live
     */
    public Mono<Void> send(String correlationId, byte[] body) {
        return sendOn(nextConnection(), correlationId, body);
    }

    /**
     * Sends a framed message on a specific connection — used to reply to an unsolicited
     * push on the exact socket it arrived on, rather than round-robining.
     */
    private Mono<Void> sendOn(Connection conn, String correlationId, byte[] body) {
        byte[] frame = buildFrame(correlationId, body);
        log.debug("Pool '{}' connId={} corrId='{}' — sending {} bytes",
                name, connId(conn), correlationId, body.length);
        /*
         * LengthFieldPrepender in the pipeline will prepend the 4-byte length
         * header automatically, so we only pass [corrId][body] here.
         */
        return conn.outbound()
                .sendByteArray(Mono.just(frame))
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

    /** Builds {@code [12-byte corrId][body]} — the length prefix is added by the pipeline. */
    private static byte[] buildFrame(String correlationId, byte[] body) {
        byte[] frame = new byte[CORR_ID_LENGTH + body.length];

        // Write correlation ID: truncate if too long, space-pad if too short
        byte[] idBytes = correlationId.getBytes(StandardCharsets.US_ASCII);
        int copyLen = Math.min(idBytes.length, CORR_ID_LENGTH);
        System.arraycopy(idBytes, 0, frame, 0, copyLen);
        Arrays.fill(frame, copyLen, CORR_ID_LENGTH, (byte) ' ');

        // Write body
        System.arraycopy(body, 0, frame, CORR_ID_LENGTH, body.length);
        return frame;
    }

    // -------------------------------------------------------------------------
    // Accessors
    // -------------------------------------------------------------------------

    public int liveConnectionCount() { return connections.size(); }
}
