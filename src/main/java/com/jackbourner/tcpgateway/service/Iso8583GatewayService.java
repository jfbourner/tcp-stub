package com.jackbourner.tcpgateway.service;

import com.jackbourner.iso8583.codec.Mappers;
import com.jackbourner.iso8583.exceptions.Mti9624Exception;
import com.jackbourner.iso8583.models.*;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeoutException;

import static com.jackbourner.iso8583.protocol.Constants.*;
import static com.jackbourner.iso8583.protocol.MessageParser.extractMessageType;

@Service
@Log4j2
public class Iso8583GatewayService {

    private final TcpDispatchService dispatchService;

    public Iso8583GatewayService(TcpDispatchService dispatchService) {
        this.dispatchService = dispatchService;
    }

    public PaymentResponseFromBankToCI_9210 sendRequest(PaymentTypes ppg, FpsMessage request)
            throws TcpDispatchService.DispatchException, InterruptedException, TimeoutException {
        FpsMessage response = dispatch(ppg, request);
        log.info("Response: {}", response);
        return toPaymentResponse(response);
    }

    private FpsMessage dispatch(PaymentTypes ppg, FpsMessage request)
            throws TcpDispatchService.DispatchException, InterruptedException, TimeoutException {
        byte[] responseBytes = dispatchService.dispatch(ppg, request.toIsoBytes());
        return parseResponse(responseBytes);
    }

    private PaymentResponseFromBankToCI_9210 toPaymentResponse(FpsMessage response) {
        if (PAYMENT_RESPONSE_9210.equals(response.getMti())) {
            return (PaymentResponseFromBankToCI_9210) response;
        }
        throw new Mti9624Exception(response, "bad response from bank");
    }

    public FpsMessage parseResponse(byte[] response) {
        String mti = extractMessageType(response);
        return switch (mti) {
            case PAYMENT_RESPONSE_9210             -> Mappers.PAYMENT_RESPONSE_BANK_TO_CI_MAPPER.fromBytes(response);
            case PAYMENT_REVERSAL_RESPONSE_9430    -> Mappers.PAYMENT_REVERSAL_RESPONSE_MAPPER.fromBytes(response);
            case NETWORK_MESSAGE_9814              -> Mappers.NETWORK_MANAGEMENT_RESPONSE_MAPPER.fromBytes(response);
            case ADMINISTRATION_ADVICE_9624        -> Mappers.ADMIN_ADVICE_MAPPER.fromBytes(response);
            case UNSOLICITED_MESSAGE_RESPONSE_9834 -> Mappers.UNSOLICITED_9824_FACTORY_MAPPER.fromBytes(response);
            default -> throw new IllegalArgumentException("Unknown MTI: " + mti);
        };
    }
}
