package com.cemear.fila_conferencia_api.conferencia.sse;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
public class SseHub {

    private final ConcurrentHashMap<String, SseEmitter> emitters = new ConcurrentHashMap<>();

    public SseEmitter addClient() {
        SseEmitter emitter = new SseEmitter(0L);

        String id = UUID.randomUUID().toString();
        emitters.put(id, emitter);

        log.info("📡 [SSE_CONNECT] clientId={} totalClients={}", id, emitters.size());

        emitter.onCompletion(() -> {
            emitters.remove(id);
            log.info("🔌 [SSE_COMPLETE] clientId={} totalClients={}", id, emitters.size());
        });

        emitter.onTimeout(() -> {
            emitters.remove(id);
            log.warn("⏱️ [SSE_TIMEOUT] clientId={} totalClients={}", id, emitters.size());
        });

        emitter.onError((ex) -> {
            emitters.remove(id);
            log.warn("⚠️ [SSE_ERROR] clientId={} totalClients={} error={}",
                    id,
                    emitters.size(),
                    ex != null ? ex.getMessage() : "unknown"
            );
        });

        safeSend(emitter, "connected", Map.of(
                "clientId", id,
                "ts", Instant.now().toString()
        ));

        return emitter;
    }

    public void broadcast(String eventName, Object data) {
        int total = emitters.size();

        log.info("📤 [SSE_BROADCAST_START] event={} clients={}", eventName, total);

        if (total == 0) {
            log.warn("⚠️ [SSE_BROADCAST_NO_CLIENTS] event={}", eventName);
            return;
        }

        emitters.forEach((id, emitter) -> {
            try {
                emitter.send(SseEmitter.event().name(eventName).data(data));
                log.info("✅ [SSE_SENT] event={} clientId={}", eventName, id);
            } catch (Exception e) {
                emitters.remove(id);

                try {
                    emitter.complete();
                } catch (Exception ignored) {
                    // conexão já morreu
                }

                log.warn("❌ [SSE_SEND_FAILED] event={} clientId={} totalClients={} error={}",
                        eventName,
                        id,
                        emitters.size(),
                        e.getMessage()
                );
            }
        });
    }

    public void heartbeat() {
        broadcast("ping", Map.of("ts", Instant.now().toString()));
    }

    public int countClients() {
        return emitters.size();
    }

    private void safeSend(SseEmitter emitter, String eventName, Object data) {
        try {
            emitter.send(SseEmitter.event().name(eventName).data(data));

            log.info("✅ [SSE_SAFE_SENT] event={}", eventName);
        } catch (IOException e) {
            log.warn("⚠️ [SSE_SAFE_SEND_FAILED] event={} error={}",
                    eventName,
                    e.getMessage()
            );
        }
    }
}