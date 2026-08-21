package com.jackbourner.tcpgateway.controller;

import com.jackbourner.iso8583.factory.FpsMessageFactory;
import com.jackbourner.iso8583.models.FpsMessage;
import com.jackbourner.iso8583.models.PaymentRequestFromCIToBank_920x;
import com.jackbourner.iso8583.models.PaymentResponseFromBankToCI_9210;
import com.jackbourner.iso8583.models.PaymentReversalFromCIToBank_9420;
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

import static com.jackbourner.iso8583.protocol.Constants.*;
import static com.jackbourner.iso8583.protocol.FieldDescriptions.describePaymentTypeCode;

/**
 * ISO 8583 JSON endpoints for the payment flow plus test-trigger endpoints that build
 * and send a synthetic message for every other outbound MTI/function-code this service
 * supports — useful for exercising a pool (typically {@code USM_IN} for the network
 * management ones) without needing a hand-built request body.
 *
 * <p>Naming: the trigger endpoints below use {@code /{ppg}/iso8583/{action}}, not the
 * older {@code /inbound/{ppg}/iso8583/generatePayment} pattern that {@link
 * #generatePayment} already uses — flagging the inconsistency rather than silently
 * carrying it forward.
 */
@RestController
@Log4j2
public class Iso8583GatewayController {

    private final Iso8583GatewayService iso8583Service;

    public Iso8583GatewayController(Iso8583GatewayService iso8583Service) {
        this.iso8583Service = iso8583Service;
    }

    // -------------------------------------------------------------------------
    // Payment (9200 → 9210)
    // -------------------------------------------------------------------------

    @PostMapping(
            value = "/{ppg}/iso8583/payment",
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<PaymentResponseFromBankToCI_9210> payment(
            @PathVariable PaymentTypes ppg,
            @RequestBody PaymentRequestFromCIToBank_920x req)
            throws TcpDispatchService.DispatchException, InterruptedException, TimeoutException {
        return ResponseEntity.ok(iso8583Service.sendRequest(ppg, req));
    }

    @PostMapping(
            value = "/{ppg}/iso8583/generatePayment",
            produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<PaymentResponseFromBankToCI_9210> generatePayment(
            @PathVariable PaymentTypes ppg)
            throws TcpDispatchService.DispatchException, InterruptedException, TimeoutException {
        log.info("ISO 8583 test request - ppg={}", ppg);
        FpsMessage request = FpsMessageFactory.paymentRequest(ppg);
        log.info("Sending ISO 8583 test request - ppg={}", ppg);
        return ResponseEntity.ok(iso8583Service.sendRequest(ppg, request));
    }

    // -------------------------------------------------------------------------
    // Network management (9804 → 9814) — one endpoint per function code
    // -------------------------------------------------------------------------

    @PostMapping(value = "/{ppg}/iso8583/sign-on", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<FpsMessage> signOn(@PathVariable PaymentTypes ppg)
            throws TcpDispatchService.DispatchException, InterruptedException, TimeoutException {
        return networkManagement(ppg, SIGN_ON_TO_SUBMIT_881);
    }

    @PostMapping(value = "/{ppg}/iso8583/sign-on-receive", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<FpsMessage> signOnReceive(@PathVariable PaymentTypes ppg)
            throws TcpDispatchService.DispatchException, InterruptedException, TimeoutException {
        return networkManagement(ppg, SIGN_ON_TO_RECEIVE_891);
    }

    @PostMapping(value = "/{ppg}/iso8583/sign-off", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<FpsMessage> signOff(@PathVariable PaymentTypes ppg)
            throws TcpDispatchService.DispatchException, InterruptedException, TimeoutException {
        return networkManagement(ppg, SIGN_OFF_TO_SUBMIT_882);
    }

    @PostMapping(value = "/{ppg}/iso8583/sign-off-receive", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<FpsMessage> signOffReceive(@PathVariable PaymentTypes ppg)
            throws TcpDispatchService.DispatchException, InterruptedException, TimeoutException {
        return networkManagement(ppg, SIGN_OFF_TO_RECEIVE_892);
    }

    @PostMapping(value = "/{ppg}/iso8583/key-change", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<FpsMessage> keyChange(@PathVariable PaymentTypes ppg)
            throws TcpDispatchService.DispatchException, InterruptedException, TimeoutException {
        return networkManagement(ppg, KEY_CHANGE_811);
    }

    @PostMapping(value = "/{ppg}/iso8583/new-key-request", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<FpsMessage> newKeyRequest(@PathVariable PaymentTypes ppg)
            throws TcpDispatchService.DispatchException, InterruptedException, TimeoutException {
        return networkManagement(ppg, NEW_KEY_REQUEST_885);
    }

    @PostMapping(value = "/{ppg}/iso8583/echo-test", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<FpsMessage> echoTest(@PathVariable PaymentTypes ppg)
            throws TcpDispatchService.DispatchException, InterruptedException, TimeoutException {
        return networkManagement(ppg, ECHO_TEST_831);
    }

    @PostMapping(value = "/{ppg}/iso8583/key-verification", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<FpsMessage> keyVerification(@PathVariable PaymentTypes ppg)
            throws TcpDispatchService.DispatchException, InterruptedException, TimeoutException {
        return networkManagement(ppg, KEY_VERIFICATION_886);
    }

    private ResponseEntity<FpsMessage> networkManagement(PaymentTypes ppg, String functionCode)
            throws TcpDispatchService.DispatchException, InterruptedException, TimeoutException {
        FpsMessage request = FpsMessageFactory.networkManagementRequest(functionCode);
        log.info("ISO 8583 network management request - ppg={} functionCode={}", ppg, functionCode);
        return ResponseEntity.ok(iso8583Service.send(ppg, request));
    }

    // -------------------------------------------------------------------------
    // Payment repeat (9201) and reversal (9420 → 9430, 9421 repeat)
    // -------------------------------------------------------------------------

    @PostMapping(value = "/{ppg}/iso8583/repeat", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<FpsMessage> repeat(@PathVariable PaymentTypes ppg)
            throws TcpDispatchService.DispatchException, InterruptedException, TimeoutException {
        PaymentRequestFromCIToBank_920x request = FpsMessageFactory.paymentRequest(ppg);
        request.setMti(PAYMENT_REPEAT_REQUEST_9201);
        log.info("ISO 8583 payment repeat request - ppg={}", ppg);
        return ResponseEntity.ok(iso8583Service.send(ppg, request));
    }

    @PostMapping(value = "/{ppg}/iso8583/reversal", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<FpsMessage> reversal(@PathVariable PaymentTypes ppg)
            throws TcpDispatchService.DispatchException, InterruptedException, TimeoutException {
        PaymentReversalFromCIToBank_9420 request = FpsMessageFactory.paymentReversal(ppg);
        log.info("ISO 8583 payment reversal request - ppg={}", ppg);
        return ResponseEntity.ok(iso8583Service.send(ppg, request));
    }

    @PostMapping(value = "/{ppg}/iso8583/reversal-repeat", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<FpsMessage> reversalRepeat(@PathVariable PaymentTypes ppg)
            throws TcpDispatchService.DispatchException, InterruptedException, TimeoutException {
        PaymentReversalFromCIToBank_9420 request = FpsMessageFactory.paymentReversal(ppg);
        request.setMti(PAYMENT_REVERSAL_REPEAT_9421);
        log.info("ISO 8583 payment reversal repeat request - ppg={}", ppg);
        return ResponseEntity.ok(iso8583Service.send(ppg, request));
    }

    // -------------------------------------------------------------------------
    // Other payment-type variants (9200, processing code overridden) — the same
    // types OTH_IN/OTH_OUT exist to bucket, per Constants' processing-code list.
    // -------------------------------------------------------------------------

    @PostMapping(value = "/{ppg}/iso8583/return", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<FpsMessage> returnPayment(@PathVariable PaymentTypes ppg)
            throws TcpDispatchService.DispatchException, InterruptedException, TimeoutException {
        return paymentWithProcessingCodeType(ppg, RETURN_PAYMENT);
    }

    @PostMapping(value = "/{ppg}/iso8583/scheme-return", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<FpsMessage> schemeReturn(@PathVariable PaymentTypes ppg)
            throws TcpDispatchService.DispatchException, InterruptedException, TimeoutException {
        return paymentWithProcessingCodeType(ppg, SCHEME_RETURN_PAYMENT);
    }

    @PostMapping(value = "/{ppg}/iso8583/bulk-auth", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<FpsMessage> bulkAuth(@PathVariable PaymentTypes ppg)
            throws TcpDispatchService.DispatchException, InterruptedException, TimeoutException {
        return paymentWithProcessingCodeType(ppg, BULK_AUTHORISATION);
    }

    @PostMapping(value = "/{ppg}/iso8583/forward-dated", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<FpsMessage> forwardDated(@PathVariable PaymentTypes ppg)
            throws TcpDispatchService.DispatchException, InterruptedException, TimeoutException {
        return paymentWithProcessingCodeType(ppg, FORWARD_DATED_PAYMENT);
    }

    @PostMapping(value = "/{ppg}/iso8583/corporate-bulk", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<FpsMessage> corporateBulk(@PathVariable PaymentTypes ppg)
            throws TcpDispatchService.DispatchException, InterruptedException, TimeoutException {
        return paymentWithProcessingCodeType(ppg, CORPORATE_BULK_PAYMENT);
    }

    /**
     * Builds the standard test payment request, then overrides just the processing-code
     * payment-type digits — a simplified trigger, not a fully populated real return/bulk
     * message (e.g. a real return would also carry {@code returnedPaymentFpid}).
     */
    private ResponseEntity<FpsMessage> paymentWithProcessingCodeType(PaymentTypes ppg, String paymentTypeCode)
            throws TcpDispatchService.DispatchException, InterruptedException, TimeoutException {
        PaymentRequestFromCIToBank_920x request = FpsMessageFactory.paymentRequest(ppg);
        request.getProcessingCode().setPaymentTypeCode(paymentTypeCode);
        request.getProcessingCode().setPaymentTypeCodeDescription(describePaymentTypeCode(paymentTypeCode));
        log.info("ISO 8583 test request - ppg={} paymentTypeCode={}", ppg, paymentTypeCode);
        return ResponseEntity.ok(iso8583Service.send(ppg, request));
    }
}
