package com.cemear.fila_conferencia_api.conferencia.sse;

import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class SseHeartbeatScheduler {

    private final SseHub sseHub;

    @Scheduled(fixedDelay = 15000) // 15s
    public void ping() {
        sseHub.heartbeat();
    }
}
