package com.jackbourner.tcpgateway.correlation;

import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Maps Transaction Reference Numbers (TRN) to pending CompletableFutures.
 * <p>
 * The TRN is the only identifier carried on the wire, so it doubles as the
 * correlation key here. The gateway also generates a short-lived, internal-only
 * correlation ID per dispatch purely for log tracing (see {@code TcpDispatchService}) —
 * that ID never goes over the wire and is not used for matching.
 * <p>
 * Lifecycle:
 *   1. Caller registers the outbound request's TRN before sending.
 *   2. The TCP read loop calls complete() with the TRN parsed from the matching response.
 *   3. On timeout the caller calls remove() to clean up.
 */
@Component
public class CorrelationRegistry {

    private final ConcurrentHashMap<String, CompletableFuture<byte[]>> pending =
            new ConcurrentHashMap<>();

    /** Register a new pending request keyed by TRN. Returns the future the caller should block on. */
    public CompletableFuture<byte[]> register(String trn) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        pending.put(trn, future);
        return future;
    }

    /**
     * Complete the future for this TRN with the supplied payload.
     *
     * @return true if a matching pending request was found, false if unsolicited
     */
    public boolean complete(String trn, byte[] payload) {
        CompletableFuture<byte[]> future = pending.remove(trn);
        if (future != null) {
            future.complete(payload);
            return true;
        }
        return false;
    }

    /** Remove a registration without completing it (e.g. after a timeout). */
    public void remove(String trn) {
        pending.remove(trn);
    }
}
