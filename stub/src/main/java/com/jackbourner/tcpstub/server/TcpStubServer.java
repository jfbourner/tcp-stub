package com.jackbourner.tcpstub.server;

import com.jackbourner.tcpstub.config.StubProperties;
import com.jackbourner.tcpstub.iso8583.FpsCodec;
import com.jackbourner.tcpstub.iso8583.FpsResponseFactory;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.jpos.iso.ISOException;
import org.jpos.iso.ISOMsg;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.tcp.TcpServer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Starts one Reactor Netty {@link TcpServer} per configured listener.
 *
 * <h3>Wire format</h3>
 * <pre>
 *   [4 bytes length] [12 bytes corrId] [ISO 8583 body]
 * </pre>
 * {@code LengthFieldBasedFrameDecoder} strips the 4-byte prefix so {@code processFrame}
 * receives {@code [12-byte corrId][ISO 8583 body]}.
 *
 * <h3>Processing order</h3>
 * <ol>
 *   <li>If {@link StubBehavior} mode is {@code DROP} → discard, no reply.</li>
 *   <li>If {@link StubBehavior} mode is {@code FIXED} → reply with the configured bytes.</li>
 *   <li>Otherwise (default {@code ECHO} mode) → parse the body as ISO 8583:
 *       <ul>
 *         <li>MTI {@code 9200} → {@link FpsResponseFactory} builds a {@code 9210} acceptance
 *             and sends it back on the same connection with the original corrId.</li>
 *         <li>Parse failure or any other MTI → echo the raw body unchanged.</li>
 *       </ul>
 *   </li>
 * </ol>
 * The {@link StubBehavior} one-shot queue lets tests inject a single DROP, delay, or
 * fixed response into the stream while leaving all other messages on the happy path.
 */
@Component
public class TcpStubServer {

    private static final Logger log = LoggerFactory.getLogger(TcpStubServer.class);

    static final int CORR_ID_LENGTH = 12;
    private static final int MAX_FRAME_SIZE = 4 * 1024 * 1024;

    private final StubProperties properties;
    private final StubBehavior behavior;
    private final StubConnectionRegistry connectionRegistry;
    private final RequestLog requestLog;
    private final FpsCodec fpsCodec;
    private final FpsResponseFactory responseFactory;

    private final List<DisposableServer> servers = new ArrayList<>();

    public TcpStubServer(StubProperties properties,
                         StubBehavior behavior,
                         StubConnectionRegistry connectionRegistry,
                         RequestLog requestLog,
                         FpsCodec fpsCodec,
                         FpsResponseFactory responseFactory) {
        this.properties = properties;
        this.behavior = behavior;
        this.connectionRegistry = connectionRegistry;
        this.requestLog = requestLog;
        this.fpsCodec = fpsCodec;
        this.responseFactory = responseFactory;
    }

    // ─── Lifecycle ────────────────────────────────────────────────────────────

    @PostConstruct
    public void start() {
        properties.getListeners().forEach((name, config) -> {
            log.info("Binding TCP stub listener '{}' on port {}", name, config.getPort());

            DisposableServer server = TcpServer.create()
                    .port(config.getPort())
                    .doOnConnection(conn -> {
                        conn.addHandlerLast("frameDecoder",
                                new LengthFieldBasedFrameDecoder(MAX_FRAME_SIZE, 0, 4, 0, 4));
                        conn.addHandlerFirst("framePrepender",
                                new LengthFieldPrepender(4));
                        connectionRegistry.add(name, conn);
                        conn.onDispose(() -> {
                            connectionRegistry.remove(name, conn);
                            log.info("Stub '{}': client disconnected (remaining={})",
                                    name, connectionRegistry.count(name));
                        });
                        log.info("Stub '{}': client connected (total={})",
                                name, connectionRegistry.count(name));
                    })
                    .handle((inbound, outbound) ->
                            inbound.receive()
                                    .retain()
                                    .asByteArray()
                                    .flatMap(frame -> processFrame(name, frame, outbound))
                                    .then())
                    .bindNow();

            servers.add(server);
            log.info("TCP stub listener '{}' ready on port {}", name, config.getPort());
        });
    }

    @PreDestroy
    public void stop() {
        servers.forEach(DisposableServer::dispose);
        log.info("All TCP stub listeners stopped");
    }

    // ─── Frame processing ─────────────────────────────────────────────────────

    private Mono<Void> processFrame(String listener, byte[] frame,
                                    reactor.netty.NettyOutbound outbound) {
        if (frame.length < CORR_ID_LENGTH) {
            log.warn("Stub '{}': undersized frame ({} bytes) — discarding", listener, frame.length);
            return Mono.empty();
        }

        String corrId = extractCorrId(frame);
        byte[] requestBody = Arrays.copyOfRange(frame, CORR_ID_LENGTH, frame.length);

        // ── 1. Check StubBehavior override (DROP / FIXED take absolute priority) ──
        StubBehavior.Decision decision = behavior.decide();

        if (decision.mode() == StubBehavior.Mode.DROP) {
            log.info("Stub '{}': DROP corrId='{}'", listener, corrId);
            requestLog.record(listener, corrId, requestBody, null, StubBehavior.Mode.DROP);
            return Mono.empty();
        }

        if (decision.mode() == StubBehavior.Mode.FIXED) {
            log.info("Stub '{}': FIXED corrId='{}'", listener, corrId);
            byte[] responseBody = decision.responseBody();
            requestLog.record(listener, corrId, requestBody, responseBody, StubBehavior.Mode.FIXED);
            return sendFrame(corrId, responseBody, decision.delayMs(), outbound);
        }

        // ── 2. ECHO mode: try to parse as ISO 8583 ───────────────────────────────
        return handleIso8583OrEcho(listener, corrId, requestBody, decision.delayMs(), outbound);
    }

    /**
     * Attempts to decode the body as a FPS ISO 8583 message.
     * <ul>
     *   <li>MTI {@code 9200} → builds and sends a {@code 9210} acceptance.</li>
     *   <li>Any other valid MTI → echoes the raw body (unsolicited / admin messages).</li>
     *   <li>Parse failure → echoes the raw body with a warning.</li>
     * </ul>
     */
    private Mono<Void> handleIso8583OrEcho(String listener, String corrId,
                                            byte[] requestBody, long delayMs,
                                            reactor.netty.NettyOutbound outbound) {
        ISOMsg request;
        try {
            request = fpsCodec.decode(requestBody);
        } catch (ISOException e) {
            log.warn("Stub '{}': corrId='{}' — body is not valid ISO 8583 ({}), echoing raw",
                    listener, corrId, e.getMessage());
            requestLog.record(listener, corrId, requestBody, requestBody, StubBehavior.Mode.ECHO);
            return sendFrame(corrId, requestBody, delayMs, outbound);
        }

        String mti = mtiOf(request);
        log.debug("Stub '{}': corrId='{}' MTI={}", listener, corrId, mti);

        if ("9200".equals(mti)) {
            return handle9200(listener, corrId, request, requestBody, delayMs, outbound);
        }

        // Non-9200 ISO 8583 message — echo it back unchanged
        log.info("Stub '{}': corrId='{}' MTI={} — echoing (no 9210 mapping for this MTI)",
                listener, corrId, mti);
        requestLog.record(listener, corrId, requestBody, requestBody, StubBehavior.Mode.ECHO);
        return sendFrame(corrId, requestBody, delayMs, outbound);
    }

    /**
     * Builds a 9210 acceptance for the received 9200, encodes it, and sends it back
     * on the same connection with the original corrId.
     */
    private Mono<Void> handle9200(String listener, String corrId, ISOMsg request,
                                   byte[] requestBody, long delayMs,
                                   reactor.netty.NettyOutbound outbound) {
        byte[] responseBody;
        try {
            ISOMsg response = responseFactory.buildAcceptance(request);
            responseBody = fpsCodec.encode(response);
        } catch (ISOException e) {
            log.error("Stub '{}': corrId='{}' — failed to build 9210 response: {}",
                    listener, corrId, e.getMessage());
            requestLog.record(listener, corrId, requestBody, null, StubBehavior.Mode.DROP);
            return Mono.empty();
        }

        log.info("Stub '{}': corrId='{}' 9200 → 9210 (accepted, DE26=0000), respBytes={}",
                listener, corrId, responseBody.length);
        requestLog.record(listener, corrId, requestBody, responseBody, StubBehavior.Mode.ECHO);
        return sendFrame(corrId, responseBody, delayMs, outbound);
    }

    // ─── Helpers ─────────────────────────────────────────────────────────────

    private Mono<Void> sendFrame(String corrId, byte[] body, long delayMs,
                                  reactor.netty.NettyOutbound outbound) {
        byte[] frame = buildFrame(corrId, body);
        Mono<Void> send = outbound.sendByteArray(Mono.just(frame)).then();
        return delayMs > 0 ? Mono.delay(Duration.ofMillis(delayMs)).then(send) : send;
    }

    private static String mtiOf(ISOMsg msg) {
        try { return msg.getMTI(); } catch (ISOException e) { return "????"; }
    }

    static String extractCorrId(byte[] frame) {
        return new String(frame, 0, CORR_ID_LENGTH, StandardCharsets.US_ASCII).trim();
    }

    static byte[] buildFrame(String corrId, byte[] body) {
        byte[] frame = new byte[CORR_ID_LENGTH + body.length];
        byte[] idBytes = corrId.getBytes(StandardCharsets.US_ASCII);
        int copyLen = Math.min(idBytes.length, CORR_ID_LENGTH);
        System.arraycopy(idBytes, 0, frame, 0, copyLen);
        Arrays.fill(frame, copyLen, CORR_ID_LENGTH, (byte) ' ');
        System.arraycopy(body, 0, frame, CORR_ID_LENGTH, body.length);
        return frame;
    }
}
