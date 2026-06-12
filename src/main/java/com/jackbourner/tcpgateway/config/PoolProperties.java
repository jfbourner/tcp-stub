package com.jackbourner.tcpgateway.config;

import com.jackbourner.iso8583.models.PaymentTypes;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Binds the tcp-gateway.pools map from application.yml.
 * Each key is the pool name (typically the PPG identifier).
 * <p>
 * Example yaml:
 *   tcp-gateway:
 *     pools:
 *       ppg1:
 *         host: 10.0.0.1
 *         port: 9001
 *         pool-size: 5
 */
@ConfigurationProperties(prefix = "tcp-gateway")
public class PoolProperties {

    private Map<PaymentTypes, PoolConfig> pools = new LinkedHashMap<>();

    public Map<PaymentTypes, PoolConfig> getPools() { return pools; }
    public void setPools(Map<PaymentTypes, PoolConfig> pools) { this.pools = pools; }
}
