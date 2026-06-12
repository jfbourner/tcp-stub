package com.jackbourner.tcpgateway.config;

public class PoolConfig {

    private String host = "localhost";
    private int port;
    private int poolSize = 1;

    public String getHost() { return host; }
    public void setHost(String host) { this.host = host; }

    public int getPort() { return port; }
    public void setPort(int port) { this.port = port; }

    public int getPoolSize() { return poolSize; }
    public void setPoolSize(int poolSize) { this.poolSize = poolSize; }
}
