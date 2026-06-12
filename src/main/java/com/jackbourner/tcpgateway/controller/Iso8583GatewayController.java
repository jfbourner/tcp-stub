package com.jackbourner.tcpgateway.controller;

import com.jackbourner.iso8583.codec.FpsMessageFactory;
import com.jackbourner.iso8583.models.FpsMessage;
import com.jackbourner.iso8583.models.PaymentRequestFromCIToBank_920x;
import com.jackbourner.iso8583.models.PaymentResponseFromBankToCI_9210;
import com.jackbourner.iso8583.models.PaymentTypes;
import com.jackbourner.tcpgateway.service.Iso8583GatewayService;
import com.jackbourner.tcpgateway.service.TcpDispatchService;
import lombok.extern.log4j.Log4j2;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.TimeoutException;

@RestController
@Log4j2
public class Iso8583GatewayController {

    private final Iso8583GatewayService iso8583Service;

    public Iso8583GatewayController(Iso8583GatewayService iso8583Service) {
        this.iso8583Service = iso8583Service;
    }

    @PostMapping(
            value = "/{direction}/{ppg}/iso8583/payment",
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<PaymentResponseFromBankToCI_9210> payment(
            @PathVariable PaymentTypes ppg,
            @RequestBody PaymentRequestFromCIToBank_920x req)
            throws TcpDispatchService.DispatchException, InterruptedException, TimeoutException {
        return ResponseEntity.ok(iso8583Service.sendRequest(ppg, req));
    }

    @PostMapping(
            value = "/inbound/{ppg}/iso8583/generatePayment",
            produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<PaymentResponseFromBankToCI_9210> generatePayment(
            @PathVariable PaymentTypes ppg)
            throws TcpDispatchService.DispatchException, InterruptedException, TimeoutException {
        log.info("ISO 8583 test request - ppg={}", ppg);
        FpsMessage request = FpsMessageFactory.paymentRequest(ppg);
        log.info("Sending ISO 8583 test request - ppg={}", ppg);
        return ResponseEntity.ok(iso8583Service.sendRequest(ppg, request));
    }
}
