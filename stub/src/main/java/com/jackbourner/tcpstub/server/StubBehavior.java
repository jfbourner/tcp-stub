package com.jackbourner.tcpstub.server;

import org.springframework.stereotype.Component;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Optional;

/**
 * Thread-safe, runtime-configurable response behaviour for the stub.
 *
 * <p>Priority order when a frame arrives:
 * <ol>
 *   <li><b>One-shot queue</b> — if a one-shot entry was enqueued via the control API it is
 *       dequeued and used for this response only.</li>
 *   <li><b>Global mode</b> — ECHO | FIXED | DROP, optionally with a delay.</li>
 * </ol>
 */
@Component
public class StubBehavior {

    public enum Mode { ECHO, FIXED, DROP }

    /** Resolved response for a single inbound frame. */
    public record Decision(Mode mode, byte[] responseBody, long delayMs) {}

    // -------------------------------------------------------------------------
    // Global state (volatile for visibility across threads)
    // -------------------------------------------------------------------------

    private volatile Mode globalMode = Mode.ECHO;
    private volatile byte[] fixedResponse = new byte[0];
    private volatile long globalDelayMs = 0;

    // -------------------------------------------------------------------------
    // One-shot queue — each entry consumed by exactly one incoming frame
    // -------------------------------------------------------------------------

    private final Deque<Decision> oneShotQueue = new ArrayDeque<>();

    // -------------------------------------------------------------------------
    // Decision resolution
    // -------------------------------------------------------------------------

    /** Called by the read loop for every inbound frame. Thread-safe. */
    public synchronized Decision decide() {
        if (!oneShotQueue.isEmpty()) {
            return oneShotQueue.poll();
        }
        byte[] body = globalMode == Mode.FIXED ? fixedResponse : new byte[0];
        return new Decision(globalMode, body, globalDelayMs);
    }

    // -------------------------------------------------------------------------
    // Mutators (called from REST control API)
    // -------------------------------------------------------------------------

    public synchronized void setGlobalMode(Mode mode) { this.globalMode = mode; }
    public synchronized void setFixedResponse(byte[] body) { this.fixedResponse = body; }
    public synchronized void setGlobalDelayMs(long ms) { this.globalDelayMs = ms; }

    /** Enqueue a one-shot response consumed by the next single incoming frame. */
    public synchronized void enqueueOneShot(Decision decision) { oneShotQueue.add(decision); }

    /** Clears the one-shot queue and resets to ECHO with no delay. */
    public synchronized void reset() {
        globalMode = Mode.ECHO;
        fixedResponse = new byte[0];
        globalDelayMs = 0;
        oneShotQueue.clear();
    }

    // -------------------------------------------------------------------------
    // Status snapshot (for /stub/status)
    // -------------------------------------------------------------------------

    public synchronized StatusSnapshot status() {
        return new StatusSnapshot(globalMode, globalDelayMs, oneShotQueue.size());
    }

    public record StatusSnapshot(Mode mode, long delayMs, int oneShotQueueDepth) {}
}
