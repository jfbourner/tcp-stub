package com.jackbourner.tcpgateway.pool;

import com.jackbourner.iso8583.models.PaymentTypes;
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
 *   <li>Unknown ID  → {@link UnsolicitedMessageHandler#handle} for server-push messages.</li>
 * </ul>
 */

@Log4j2
public class TcpConnectionPool {

    /** Length of the correlation-ID field in every frame header (bytes). */
    static final int CORR_ID_LENGTH = 12;

    /** Maximum frame size accepted from the server (bytes, inclusive of corrId). */
    private static final int MAX_FRAME_SIZE = 4 * 1024 * 1024; // 4 MB

    private static final Duration RECONNECT_DELAY = Duration.ofSeconds(5);

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
                .doOnNext(this::routeInbound)
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
    private void routeInbound(byte[] frame) {
        if (frame.length < CORR_ID_LENGTH) {
            log.warn("Pool '{}' received undersized frame ({} bytes) — discarding", name, frame.length);
            return;
        }

        String corrId = extractCorrId(frame);
        byte[] body = Arrays.copyOfRange(frame, CORR_ID_LENGTH, frame.length);

        if (!correlationRegistry.complete(corrId, body)) {
            send(corrId, body);
            unsolicitedHandler.handle(name, corrId, body);
        }
    }

    private static String extractCorrId(byte[] frame) {
        return new String(frame, 0, CORR_ID_LENGTH, StandardCharsets.US_ASCII).trim();
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
        Connection conn = nextConnection();
        byte[] frame = buildFrame(correlationId, body);
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
