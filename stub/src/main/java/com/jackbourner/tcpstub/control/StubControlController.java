package com.jackbourner.tcpstub.control;

import com.jackbourner.tcpstub.server.RequestLog;
import com.jackbourner.tcpstub.server.RequestLogEntry;
import com.jackbourner.tcpstub.server.StubBehavior;
import com.jackbourner.tcpstub.server.StubConnectionRegistry;
import com.jackbourner.tcpstub.server.TcpStubServer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HexFormat;
import java.util.List;
import java.util.Map;

/**
 * REST control plane for the TCP stub.  All endpoints are under {@code /stub}.
 *
 * <pre>
 * GET  /stub/status                     Overall status: listeners, connections, behavior
 * GET  /stub/history?limit=50           Recent request/response log (newest first)
 * DELETE /stub/history                  Clear history
 *
 * PUT  /stub/behavior                   Set global mode/delay/fixed-response
 * POST /stub/behavior/one-shot          Enqueue a one-shot response for the next frame
 * DELETE /stub/behavior                 Reset everything to ECHO / no delay
 *
 * POST /stub/push/{listener}            Send an unsolicited frame to all connections on a listener
 * </pre>
 *
 * <h3>Binary encoding</h3>
 * All binary payloads are represented as lowercase hex strings in JSON, e.g. {@code "48656c6c6f"}.
 * Use an empty string {@code ""} for a zero-byte body.
 */
@RestController
@RequestMapping("/stub")
public class StubControlController {

    private static final HexFormat HEX = HexFormat.of();

    private final StubBehavior behavior;
    private final StubConnectionRegistry connectionRegistry;
    private final RequestLog requestLog;

    public StubControlController(StubBehavior behavior,
                                 StubConnectionRegistry connectionRegistry,
                                 RequestLog requestLog) {
        this.behavior = behavior;
        this.connectionRegistry = connectionRegistry;
        this.requestLog = requestLog;
    }

    // -------------------------------------------------------------------------
    // Status
    // -------------------------------------------------------------------------

    @GetMapping("/status")
    public Map<String, Object> status() {
        StubBehavior.StatusSnapshot snap = behavior.status();
        return Map.of(
                "connections", connectionRegistry.counts(),
                "behavior", Map.of(
                        "mode", snap.mode(),
                        "delayMs", snap.delayMs(),
                        "oneShotQueueDepth", snap.oneShotQueueDepth()
                )
        );
    }

    // -------------------------------------------------------------------------
    // History
    // -------------------------------------------------------------------------

    @GetMapping("/history")
    public List<Map<String, Object>> history(@RequestParam(defaultValue = "50") int limit) {
        return requestLog.recent(limit).stream()
                .map(this::toJson)
                .toList();
    }

    @DeleteMapping("/history")
    public ResponseEntity<Void> clearHistory() {
        requestLog.clear();
        return ResponseEntity.noContent().build();
    }

    // -------------------------------------------------------------------------
    // Behavior control
    // -------------------------------------------------------------------------

    /**
     * Set global behavior.
     *
     * <pre>
     * {
     *   "mode": "ECHO" | "FIXED" | "DROP",
     *   "delayMs": 500,
     *   "fixedResponseHex": "48656c6c6f"
     * }
     * </pre>
     */
    @PutMapping("/behavior")
    public Map<String, Object> setBehavior(@RequestBody BehaviorRequest req) {
        StubBehavior.Mode mode = StubBehavior.Mode.valueOf(req.mode().toUpperCase());
        behavior.setGlobalMode(mode);
        behavior.setGlobalDelayMs(req.delayMs());
        if (req.fixedResponseHex() != null && !req.fixedResponseHex().isBlank()) {
            behavior.setFixedResponse(HEX.parseHex(req.fixedResponseHex()));
        }
        return Map.of("applied", behavior.status());
    }

    /**
     * Enqueue a one-shot decision consumed by the very next inbound frame.
     * Useful for injecting a single delayed or drop response mid-test.
     *
     * <pre>
     * {
     *   "mode": "DROP",
     *   "delayMs": 0,
     *   "fixedResponseHex": ""
     * }
     * </pre>
     */
    @PostMapping("/behavior/one-shot")
    public Map<String, Object> enqueueOneShot(@RequestBody BehaviorRequest req) {
        StubBehavior.Mode mode = StubBehavior.Mode.valueOf(req.mode().toUpperCase());
        byte[] body = (req.fixedResponseHex() != null && !req.fixedResponseHex().isBlank())
                ? HEX.parseHex(req.fixedResponseHex())
                : new byte[0];
        StubBehavior.Decision d = new StubBehavior.Decision(mode, body, req.delayMs());
        behavior.enqueueOneShot(d);
        return Map.of("queued", behavior.status());
    }

    @DeleteMapping("/behavior")
    public Map<String, Object> resetBehavior() {
        behavior.reset();
        return Map.of("reset", behavior.status());
    }

    // -------------------------------------------------------------------------
    // Push (unsolicited server→gateway messages)
    // -------------------------------------------------------------------------

    /**
     * Sends an unsolicited frame to every connected gateway instance on a listener.
     * The gateway's read loop will look up the corrId in its CorrelationRegistry —
     * if not found it routes to {@code UnsolicitedMessageHandler}.
     *
     * <pre>
     * {
     *   "corrId": "push001",
     *   "payloadHex": "48656c6c6f"
     * }
     * </pre>
     */
    @PostMapping("/push/{listener}")
    public ResponseEntity<Map<String, Object>> push(
            @PathVariable String listener,
            @RequestBody PushRequest req) {

        byte[] payload = req.payloadHex() != null && !req.payloadHex().isBlank()
                ? HEX.parseHex(req.payloadHex())
                : new byte[0];

        byte[] frame = TcpStubServer.buildFrame(req.corrId(), payload);
        connectionRegistry.broadcast(listener, frame);

        return ResponseEntity.ok(Map.of(
                "listener", listener,
                "corrId", req.corrId(),
                "payloadBytes", payload.length,
                "sentToConnections", connectionRegistry.count(listener)
        ));
    }

    // -------------------------------------------------------------------------
    // JSON helpers
    // -------------------------------------------------------------------------

    private Map<String, Object> toJson(RequestLogEntry e) {
        return Map.of(
                "timestamp", e.timestamp().toString(),
                "listener", e.listener(),
                "corrId", e.corrId(),
                "mode", e.mode().name(),
                "requestHex", HEX.formatHex(e.requestBody()),
                "responseHex", e.responseBody() != null ? HEX.formatHex(e.responseBody()) : ""
        );
    }

    // -------------------------------------------------------------------------
    // Request DTOs
    // -------------------------------------------------------------------------

    record BehaviorRequest(String mode, long delayMs, String fixedResponseHex) {}
    record PushRequest(String corrId, String payloadHex) {}
}
