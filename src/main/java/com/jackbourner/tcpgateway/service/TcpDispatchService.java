package com.jackbourner.tcpgateway.service;

import com.jackbourner.iso8583.models.PaymentTypes;
import com.jackbourner.tcpgateway.correlation.CorrelationRegistry;
import com.jackbourner.tcpgateway.pool.TcpConnectionPoolManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Shared send-and-wait logic used by all gateway controllers.
 * <p>
 * Picks the next available connection from the named pool, registers a correlation ID,
 * fires the bytes over TCP, then blocks until the correlated response arrives or the
 * timeout elapses.
 */
@Service
public class TcpDispatchService {

    private static final Logger log = LoggerFactory.getLogger(TcpDispatchService.class);

    private final TcpConnectionPoolManager poolManager;
    private final CorrelationRegistry correlationRegistry;
    private final long responseTimeoutMs;

    public TcpDispatchService(TcpConnectionPoolManager poolManager,
                              CorrelationRegistry correlationRegistry,
                              @Value("${tcp-gateway.response-timeout-ms:30000}") long responseTimeoutMs) {
        this.poolManager = poolManager;
        this.correlationRegistry = correlationRegistry;
        this.responseTimeoutMs = responseTimeoutMs;
    }

    /**
     * Sends {@code payload} to the pool named {@code poolName} and blocks until the
     * correlated response byte array arrives.
     *
     * @throws IllegalArgumentException if no pool is configured for {@code poolName}
     * @throws IllegalStateException    if the pool has no live connections
     * @throws TimeoutException         if no response arrives within the configured timeout
     * @throws InterruptedException     if the calling thread is interrupted while waiting
     * @throws DispatchException        wraps any other send-side error
     */
    public byte[] dispatch(PaymentTypes paymentType, byte[] payload)
            throws TimeoutException, InterruptedException, DispatchException {

        String corrId = generateCorrId();
        log.debug("paymentType pool='{}' corrId={} payloadBytes={}", paymentType, corrId, payload.length);

        CompletableFuture<byte[]> future = correlationRegistry.register(corrId);

        try {
            poolManager.getPool(paymentType)
                    .send(corrId, payload)
                    .doOnError(err -> {
                        log.error("Send error corrId={}: {}", corrId, err.getMessage());
                        correlationRegistry.remove(corrId);
                        future.completeExceptionally(err);
                    })
                    .subscribe();

            return future.get(responseTimeoutMs, TimeUnit.MILLISECONDS);

        } catch (TimeoutException | InterruptedException e) {
            correlationRegistry.remove(corrId);
            throw e;
        } catch (java.util.concurrent.ExecutionException e) {
            throw new DispatchException("Send failed for corrId=" + corrId, e.getCause());
        }
    }

    private static String generateCorrId() {
        return UUID.randomUUID().toString().replace("-", "").substring(0, 12);
    }

    // -------------------------------------------------------------------------

    public static class DispatchException extends Exception {
        public DispatchException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
