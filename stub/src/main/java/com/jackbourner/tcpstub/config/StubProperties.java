package com.jackbourner.tcpstub.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Binds {@code tcp-stub.*} from application.yml.
 *
 * Example:
 * <pre>
 * tcp-stub:
 *   listeners:
 *     ppg1:
 *       port: 9001
 *     ppg2:
 *       port: 9002
 *   history-max-size: 200
 * </pre>
 */
@ConfigurationProperties(prefix = "tcp-stub")
public class StubProperties {

    private Map<String, ListenerConfig> listeners = new LinkedHashMap<>();
    private int historyMaxSize = 200;

    public Map<String, ListenerConfig> getListeners() { return listeners; }
    public void setListeners(Map<String, ListenerConfig> listeners) { this.listeners = listeners; }

    public int getHistoryMaxSize() { return historyMaxSize; }
    public void setHistoryMaxSize(int historyMaxSize) { this.historyMaxSize = historyMaxSize; }

    public static class ListenerConfig {
        private int port;

        public int getPort() { return port; }
        public void setPort(int port) { this.port = port; }
    }
}
