package com.jackbourner.tcpstub.server;

import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * Bounded in-memory log of stub exchanges.  Oldest entries are evicted once the
 * configured capacity is reached.  Thread-safe via {@code synchronized}.
 */
@Component
public class RequestLog {

    private final int maxSize;
    private final Deque<RequestLogEntry> entries;

    public RequestLog(com.jackbourner.tcpstub.config.StubProperties properties) {
        this.maxSize = properties.getHistoryMaxSize();
        this.entries = new ArrayDeque<>(maxSize);
    }

    public synchronized void record(String listener, String corrId,
                                    byte[] requestBody, byte[] responseBody,
                                    StubBehavior.Mode mode) {
        if (entries.size() >= maxSize) {
            entries.poll();
        }
        entries.add(new RequestLogEntry(Instant.now(), listener, corrId,
                requestBody, responseBody, mode));
    }

    /** Returns a snapshot of all entries (oldest first). */
    public synchronized List<RequestLogEntry> snapshot() {
        return new ArrayList<>(entries);
    }

    /** Returns the most recent {@code limit} entries (newest first). */
    public synchronized List<RequestLogEntry> recent(int limit) {
        List<RequestLogEntry> all = new ArrayList<>(entries);
        int from = Math.max(0, all.size() - limit);
        List<RequestLogEntry> tail = new ArrayList<>(all.subList(from, all.size()));
        tail = new ArrayList<>(tail);
        java.util.Collections.reverse(tail);
        return tail;
    }

    public synchronized void clear() {
        entries.clear();
    }
}
