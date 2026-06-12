package com.jackbourner.tcpstub.iso8583;

import org.jpos.iso.ISOException;
import org.jpos.iso.ISOMsg;
import org.springframework.stereotype.Component;

/**
 * Converts an inbound 9200 payment request into a simulated 9210 acceptance response.
 *
 * <h3>Strategy</h3>
 * All fields present in the 9200 are echoed verbatim in the 9210 (the gateway reads them
 * back to populate {@code PaymentResponse}).  The following fields are added or overridden:
 * <ul>
 *   <li><b>MTI</b> — changed from "9200" to "9210"</li>
 *   <li><b>DE26 Action Code</b> — set to "0000" (accepted / no error)</li>
 * </ul>
 *
 * <h3>Action Code values (DE26)</h3>
 * <pre>
 *   0000  Accepted
 *   non-zero  Error — use {@link #buildRejection} to simulate a rejection
 * </pre>
 */
@Component
public class FpsResponseFactory {

    /** DE26 value meaning the payment was accepted by the receiving member. */
    public static final String ACTION_ACCEPTED = "0000";

    /**
     * Builds a 9210 acceptance from the supplied 9200 request.
     *
     * @param request decoded 9200 {@link ISOMsg}; must not have been packed yet
     * @return new {@link ISOMsg} with MTI "9210" and DE26 = "0000"
     */
    public ISOMsg buildAcceptance(ISOMsg request) throws ISOException {
        ISOMsg response = clone(request);
        response.setMTI("9210");
        response.set(26, ACTION_ACCEPTED);   // DE26: Action Code = accepted
        return response;
    }

    /**
     * Builds a 9210 rejection from the supplied 9200 request.
     *
     * @param request    decoded 9200
     * @param actionCode non-zero 4-digit FPS action / error code (e.g. "1220" = invalid account)
     */
    public ISOMsg buildRejection(ISOMsg request, String actionCode) throws ISOException {
        ISOMsg response = clone(request);
        response.setMTI("9210");
        response.set(26, actionCode);
        return response;
    }

    // ─────────────────────────────────────────────────────────────────────────

    private static ISOMsg clone(ISOMsg src) throws ISOException {
        try {
            return (ISOMsg) src.clone();
        } catch (CloneNotSupportedException e) {
            throw new ISOException("Failed to clone ISOMsg", e);
        }
    }
}
