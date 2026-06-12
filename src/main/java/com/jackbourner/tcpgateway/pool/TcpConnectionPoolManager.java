package com.jackbourner.tcpgateway.pool;

import com.jackbourner.iso8583.models.PaymentTypes;
import com.jackbourner.tcpgateway.config.PoolConfig;
import com.jackbourner.tcpgateway.config.PoolProperties;
import com.jackbourner.tcpgateway.correlation.CorrelationRegistry;
import com.jackbourner.tcpgateway.handler.UnsolicitedMessageHandler;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Builds one {@link TcpConnectionPool} per entry in {@code tcp-gateway.pools}
 * and exposes them for lookup by pool name (typically the PPG identifier).
 */
@Component
public class TcpConnectionPoolManager {

    private static final Logger log = LoggerFactory.getLogger(TcpConnectionPoolManager.class);

    private final Map<PaymentTypes, TcpConnectionPool> pools;

    public TcpConnectionPoolManager(PoolProperties properties,
                                    CorrelationRegistry correlationRegistry,
                                    UnsolicitedMessageHandler unsolicitedHandler) {
        Map<PaymentTypes, TcpConnectionPool> map = new LinkedHashMap<>();

        for (Map.Entry<PaymentTypes, PoolConfig> entry : properties.getPools().entrySet()) {
            PaymentTypes name = entry.getKey();
            TcpConnectionPool pool = new TcpConnectionPool(
                    name, entry.getValue(), correlationRegistry, unsolicitedHandler);
            pool.init();
            map.put(name, pool);
        }

        this.pools = Collections.unmodifiableMap(map);
        log.info("TcpConnectionPoolManager initialised with {} pool(s): {}", map.size(), map.keySet());
    }

    /**
     * Returns the pool for the given name.
     *
     * @throws IllegalArgumentException if no pool is configured for that name
     */
    public TcpConnectionPool getPool(PaymentTypes name) {
        TcpConnectionPool pool = pools.get(name);
        if (pool == null) {
            throw new IllegalArgumentException(
                    "No TCP pool configured for '" + name + "'. Known pools: " + pools.keySet());
        }
        return pool;
    }

    /** Returns an unmodifiable view of all pools, keyed by name. */
    public Map<PaymentTypes, TcpConnectionPool> getAllPools() {
        return pools;
    }

    @PreDestroy
    public void shutdown() {
        pools.values().forEach(TcpConnectionPool::close);
        log.info("All TCP pools shut down");
    }
}
