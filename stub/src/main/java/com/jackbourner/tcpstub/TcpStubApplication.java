package com.jackbourner.tcpstub;

import com.jackbourner.tcpstub.config.StubProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(StubProperties.class)
public class TcpStubApplication {

    public static void main(String[] args) {
        SpringApplication.run(TcpStubApplication.class, args);
    }
}
