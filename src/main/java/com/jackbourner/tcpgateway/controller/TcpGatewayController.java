package com.jackbourner.tcpgateway.controller;

import com.jackbourner.iso8583.models.PaymentTypes;
import com.jackbourner.tcpgateway.service.TcpDispatchService;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.TimeoutException;

/**
 * Raw binary pass-through endpoints.
 *
 * <pre>
 *   POST /{direction}/{ppg}/ci-to-bank   application/octet-stream
 *   POST /{direction}/{ppg}/              application/octet-stream
 * </pre>
 *
 * For ISO 8583 JSON endpoints see {@link Iso8583GatewayController}.
 */
@RestController
public class TcpGatewayController {

    private final TcpDispatchService dispatchService;

    public TcpGatewayController(TcpDispatchService dispatchService) {
        this.dispatchService = dispatchService;
    }

    @PostMapping(
            value = {"/{direction}/{ppg}/ci-to-bank", "/{direction}/{ppg}/"},
            consumes = MediaType.APPLICATION_OCTET_STREAM_VALUE,
            produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public ResponseEntity<byte[]> ciToBank(
            @PathVariable PaymentTypes ppg,
            @RequestBody byte[] body)
            throws TcpDispatchService.DispatchException, InterruptedException, TimeoutException {
        return ResponseEntity.ok(dispatchService.dispatch(ppg, body));
    }
}
