package com.jackbourner.tcpstub.iso8583;

import org.jpos.iso.ISOException;
import org.jpos.iso.ISOMsg;
import org.jpos.iso.packager.GenericPackager;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;

/**
 * Thin jPOS wrapper loaded from the same {@code packager/faster-payments.xml} used by
 * the gateway.  Both sides must use an identical packager to interoperate correctly.
 *
 * <p>Thread-safe after construction — the {@link GenericPackager} is stateless.
 */
@Component
public class FpsCodec {

    private static final String PACKAGER_RESOURCE = "packager/faster-payments.xml";

    private final GenericPackager packager;

    public FpsCodec() {
        try (InputStream is = new ClassPathResource(PACKAGER_RESOURCE).getInputStream()) {
            packager = new GenericPackager(is);
        } catch (ISOException | IOException e) {
            throw new IllegalStateException(
                    "Stub: failed to load packager from " + PACKAGER_RESOURCE, e);
        }
    }

    /**
     * Attempts to decode raw bytes as a FPS ISO 8583 message.
     *
     * @throws ISOException if the bytes do not conform to the packager definition
     */
    public ISOMsg decode(byte[] data) throws ISOException {
        ISOMsg msg = new ISOMsg();
        msg.setPackager(packager);
        msg.unpack(data);
        return msg;
    }

    /**
     * Packs an {@link ISOMsg} to bytes.  The packager is set on the message before
     * packing, so callers do not need to set it themselves.
     *
     * @throws ISOException if any field value violates its packager definition
     */
    public byte[] encode(ISOMsg msg) throws ISOException {
        msg.setPackager(packager);
        return msg.pack();
    }
}
