package com.jackbourner.tcpgateway;

import com.jackbourner.tcpgateway.config.PoolProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(PoolProperties.class)
public class TcpGatewayApplication {

    public static void main(String[] args) {
        SpringApplication.run(TcpGatewayApplication.class, args);
    }
}
