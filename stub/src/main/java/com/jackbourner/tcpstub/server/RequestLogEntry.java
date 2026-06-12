package com.jackbourner.tcpstub.server;

import java.time.Instant;

/**
 * Immutable record of one request/response exchange seen by the stub.
 * Binary fields are stored raw; the control API hex-encodes them for JSON output.
 */
public record RequestLogEntry(
        Instant timestamp,
        String listener,
        String corrId,
        byte[] requestBody,
        byte[] responseBody,   // null when mode is DROP
        StubBehavior.Mode mode
) {}
