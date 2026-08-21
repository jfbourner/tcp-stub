package com.jackbourner.tcpgateway.service;

import com.jackbourner.iso8583.codec.MessageParser;
import com.jackbourner.iso8583.codec.Mappers;
import com.jackbourner.iso8583.models.FpsMessage;
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

import static com.jackbourner.iso8583.protocol.Constants.PAYMENT_REPEAT_REQUEST_9201;
import static com.jackbourner.iso8583.protocol.Constants.PAYMENT_REQUEST_9200;

/**
 * Shared send-and-wait logic used by all gateway controllers.
 * <p>
 * Picks the next available connection from the named pool, registers the outbound
 * payload's Transaction Reference Number (TRN) — the only identifier carried on the
 * wire — fires the bytes over TCP, then blocks until the correlated response arrives
 * or the timeout elapses.
 * <p>
 * A short-lived, internal-only correlation ID is also generated per dispatch purely
 * for log tracing (connection id + correlation id + TRN); it never goes over the wire
 * and plays no part in matching the response.
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

        String trn;
        try {
            trn = extractTrn(payload);
        } catch (Exception e) {
            throw new DispatchException("Could not determine TRN for outbound payload", e);
        }

        String corrId = generateCorrId();
        log.debug("dispatch pool='{}' corrId={} trn='{}' payloadBytes={}", paymentType, corrId, trn, payload.length);

        CompletableFuture<byte[]> future = correlationRegistry.register(trn);

        try {
            poolManager.getPool(paymentType)
                    .send(trn, payload)
                    .doOnError(err -> {
                        log.error("Send error corrId={} trn='{}': {}", corrId, trn, err.getMessage());
                        correlationRegistry.remove(trn);
                        future.completeExceptionally(err);
                    })
                    .subscribe();

            return future.get(responseTimeoutMs, TimeUnit.MILLISECONDS);

        } catch (TimeoutException | InterruptedException e) {
            correlationRegistry.remove(trn);
            throw e;
        } catch (java.util.concurrent.ExecutionException e) {
            throw new DispatchException("Send failed for corrId=" + corrId + " trn='" + trn + "'", e.getCause());
        }
    }

    private static String generateCorrId() {
        return UUID.randomUUID().toString().replace("-", "").substring(0, 12);
    }

    /**
     * Extracts the TRN from an outbound (CI-to-bank direction) payload. 9200/9201 are
     * ambiguous MTIs (a different field set applies bank-to-CI) so they're parsed with
     * the explicit CI-to-bank mapper; every other outbound MTI this gateway sends
     * (9420/9421, 9804) is unambiguous and resolved by {@link MessageParser#parseToModel}.
     */
    private static String extractTrn(byte[] payload) {
        String mti = MessageParser.extractMessageType(payload);
        FpsMessage msg = switch (mti) {
            case PAYMENT_REQUEST_9200, PAYMENT_REPEAT_REQUEST_9201 ->
                    Mappers.PAYMENT_REQUEST_CI_TO_BANK_MAPPER.fromBytes(payload);
            default -> MessageParser.parseToModel(payload);
        };
        return msg.getTransactionReferenceNumber();
    }

    // -------------------------------------------------------------------------

    public static class DispatchException extends Exception {
        public DispatchException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
