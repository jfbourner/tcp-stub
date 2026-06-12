package com.jackbourner.tcpstub.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Tracks every live client connection, grouped by listener name.
 * Used by the control API to push unsolicited frames to connected gateway instances.
 */
@Component
public class StubConnectionRegistry {

    private static final Logger log = LoggerFactory.getLogger(StubConnectionRegistry.class);

    private final ConcurrentHashMap<String, CopyOnWriteArrayList<Connection>> byListener =
            new ConcurrentHashMap<>();

    public void add(String listener, Connection conn) {
        byListener.computeIfAbsent(listener, k -> new CopyOnWriteArrayList<>()).add(conn);
    }

    public void remove(String listener, Connection conn) {
        CopyOnWriteArrayList<Connection> list = byListener.get(listener);
        if (list != null) list.remove(conn);
    }

    public List<Connection> getConnections(String listener) {
        return Collections.unmodifiableList(
                byListener.getOrDefault(listener, new CopyOnWriteArrayList<>()));
    }

    public int count(String listener) {
        CopyOnWriteArrayList<Connection> list = byListener.get(listener);
        return list == null ? 0 : list.size();
    }

    /** Count per listener name — for status reporting. */
    public Map<String, Integer> counts() {
        ConcurrentHashMap<String, Integer> result = new ConcurrentHashMap<>();
        byListener.forEach((name, list) -> result.put(name, list.size()));
        return result;
    }

    /**
     * Sends {@code frame} to every live connection on the given listener.
     * The frame must already include the corrId header; the pipeline's
     * {@code LengthFieldPrepender} will add the 4-byte length prefix.
     */
    public void broadcast(String listener, byte[] frame) {
        List<Connection> conns = getConnections(listener);
        if (conns.isEmpty()) {
            log.warn("No connections on listener '{}' — push discarded", listener);
            return;
        }
        conns.forEach(conn ->
                conn.outbound()
                        .sendByteArray(Mono.just(frame))
                        .then()
                        .doOnError(e -> log.warn("Push error on listener '{}': {}", listener, e.getMessage()))
                        .subscribe());
        log.info("Pushed frame to {} connection(s) on listener '{}'", conns.size(), listener);
    }
}
