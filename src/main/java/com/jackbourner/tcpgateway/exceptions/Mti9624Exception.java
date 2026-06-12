package com.jackbourner.tcpgateway.exceptions;

import com.jackbourner.iso8583.models.AdminAdvice_9624;
import com.jackbourner.iso8583.models.FpsMessage;
import lombok.Getter;

@Getter
public class Mti9624Exception extends RuntimeException {

    private final AdminAdvice_9624 adminAdvice9624;

    public Mti9624Exception(FpsMessage data, String message) {
        super(message);
        this.adminAdvice9624 = (AdminAdvice_9624) data;
    }
}
