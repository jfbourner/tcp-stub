package com.jackbourner.tcpgateway;

import com.jackbourner.iso8583.models.PaymentTypes;
import com.jackbourner.tcpgateway.correlation.CorrelationRegistry;
import com.jackbourner.tcpgateway.pool.TcpConnectionPoolManager;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.test.context.ActiveProfiles;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Basic smoke-tests that do NOT require a running TCP server.
 * Integration tests that exercise the actual TCP connections should
 * spin up a local server fixture (e.g. using a mock Netty server).
 */
@SpringBootTest(
        webEnvironment = WebEnvironment.NONE,
        properties = {
            // Override pool config so no real server is needed at test time.
            // Pool init will attempt connections and log warnings — that is expected.
            "tcp-gateway.pools.SIP_IN.host=127.0.0.1",
            "tcp-gateway.pools.SIP_IN.port=19001",
            "tcp-gateway.pools.SIP_IN.pool-size=1",
            "tcp-gateway.pools.SOP_IN.host=127.0.0.1",
            "tcp-gateway.pools.SOP_IN.port=19002",
            "tcp-gateway.pools.SOP_IN.pool-size=1"
        })
@ActiveProfiles("test")
class TcpGatewayApplicationTests {

    @Autowired
    private TcpConnectionPoolManager poolManager;

    @Autowired
    private CorrelationRegistry correlationRegistry;

    @Test
    void contextLoads() {
        assertThat(poolManager).isNotNull();
        assertThat(correlationRegistry).isNotNull();
    }

    @Test
    void poolManagerKnowsConfiguredPools() {
        assertThat(poolManager.getAllPools()).containsKeys(PaymentTypes.SIP_IN, PaymentTypes.SOP_IN);
    }

    @Test
    void poolManagerThrowsForUnknownPool() {
        assertThrows(IllegalArgumentException.class, () -> poolManager.getPool(null));
    }

    @Test
    void correlationRegistry_registerCompleteRoundTrip() throws Exception {
        CompletableFuture<byte[]> future = correlationRegistry.register("testId");

        byte[] payload = "hello".getBytes();
        boolean completed = correlationRegistry.complete("testId", payload);

        assertThat(completed).isTrue();
        assertThat(future.get(1, TimeUnit.SECONDS)).isEqualTo(payload);
    }

    @Test
    void correlationRegistry_returnsFalseForUnknownId() {
        boolean completed = correlationRegistry.complete("noSuchId", new byte[0]);
        assertThat(completed).isFalse();
    }

    @Test
    void correlationRegistry_removeStopsCompletion() {
        correlationRegistry.register("removeMe");
        correlationRegistry.remove("removeMe");
        boolean completed = correlationRegistry.complete("removeMe", new byte[0]);
        assertThat(completed).isFalse();
    }
}
