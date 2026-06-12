package com.jackbourner.tcpgateway.controller;

import com.jackbourner.iso8583.models.AdminAdvice_9624;
import com.jackbourner.tcpgateway.exceptions.Mti9624Exception;
import com.jackbourner.tcpgateway.service.TcpDispatchService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

import java.util.concurrent.TimeoutException;

@ControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(Mti9624Exception.class)
    public ResponseEntity<AdminAdvice_9624> handleMti9624(Mti9624Exception ex) {
        return ResponseEntity.ok(ex.getAdminAdvice9624());
    }

    @ExceptionHandler({IllegalArgumentException.class, IllegalStateException.class})
    public ResponseEntity<String> handleBadGateway(RuntimeException ex) {
        log.warn("Dispatch rejected: {}", ex.getMessage());
        return ResponseEntity.status(HttpStatus.BAD_GATEWAY).body(ex.getMessage());
    }

    @ExceptionHandler(TimeoutException.class)
    public ResponseEntity<Void> handleTimeout(TimeoutException ex) {
        log.warn("Timeout waiting for TCP response");
        return ResponseEntity.status(HttpStatus.GATEWAY_TIMEOUT).build();
    }

    @ExceptionHandler(InterruptedException.class)
    public ResponseEntity<Void> handleInterrupted(InterruptedException ex) {
        Thread.currentThread().interrupt();
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).build();
    }

    @ExceptionHandler(TcpDispatchService.DispatchException.class)
    public ResponseEntity<Void> handleDispatch(TcpDispatchService.DispatchException ex) {
        log.error("Dispatch error", ex);
        return ResponseEntity.status(HttpStatus.BAD_GATEWAY).build();
    }
}
