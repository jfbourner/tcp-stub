package com.jackbourner.tcpgateway.correlation;

import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Maps correlation IDs (String) to pending CompletableFutures.
 * <p>
 * Lifecycle:
 *   1. Caller registers an ID before sending the request.
 *   2. The TCP read loop calls complete() when the matching response arrives.
 *   3. On timeout the caller calls remove() to clean up.
 */
@Component
public class CorrelationRegistry {

    private final ConcurrentHashMap<String, CompletableFuture<byte[]>> pending =
            new ConcurrentHashMap<>();

    /** Register a new pending request. Returns the future the caller should block on. */
    public CompletableFuture<byte[]> register(String correlationId) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        pending.put(correlationId, future);
        return future;
    }

    /**
     * Complete the future for this ID with the supplied payload.
     *
     * @return true if a matching pending request was found, false if unsolicited
     */
    public boolean complete(String correlationId, byte[] payload) {
        CompletableFuture<byte[]> future = pending.remove(correlationId);
        if (future != null) {
            future.complete(payload);
            return true;
        }
        return false;
    }

    /** Remove a registration without completing it (e.g. after a timeout). */
    public void remove(String correlationId) {
        pending.remove(correlationId);
    }
}
