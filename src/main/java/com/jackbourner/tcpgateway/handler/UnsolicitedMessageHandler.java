package com.jackbourner.tcpgateway.handler;

import com.jackbourner.iso8583.models.PaymentTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Receives server-push (unsolicited) messages — i.e. frames whose TRN (or MTI, if the
 * TRN couldn't be parsed) has no registered pending future in the CorrelationRegistry.
 * <p>
 * Override or extend this bean to add domain-specific handling (e.g. publish to
 * a queue, call a webhook, etc.).
 */
@Component
public class UnsolicitedMessageHandler {

    private static final Logger log = LoggerFactory.getLogger(UnsolicitedMessageHandler.class);

    public void handle(PaymentTypes paymentType, String trn, byte[] payload) {
        log.info("Unsolicited message on pool='{}' trn='{}' payloadBytes={}",
                paymentType, trn != null ? trn : "-", payload.length);
    }
}
