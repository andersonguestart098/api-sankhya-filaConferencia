package com.cemear.fila_conferencia_api.conferencia.sse;

import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class SseHub {

    private final ConcurrentHashMap<String, SseEmitter> emitters = new ConcurrentHashMap<>();

    public SseEmitter addClient() {
        // 0L = sem timeout (mantém conexão viva; heartbeat cuida disso)
        SseEmitter emitter = new SseEmitter(0L);

        String id = UUID.randomUUID().toString();
        emitters.put(id, emitter);

        emitter.onCompletion(() -> emitters.remove(id));
        emitter.onTimeout(() -> emitters.remove(id));
        emitter.onError((ex) -> emitters.remove(id));

        // Evento inicial: útil pra testar conexão
        safeSend(emitter, "connected", Map.of("ts", Instant.now().toString()));

        return emitter;
    }

    public void broadcast(String eventName, Object data) {
        emitters.forEach((id, emitter) -> {
            try {
                emitter.send(SseEmitter.event().name(eventName).data(data));
            } catch (Exception e) {
                emitters.remove(id);
            }
        });
    }

    public void heartbeat() {
        broadcast("ping", Map.of("ts", Instant.now().toString()));
    }

    private void safeSend(SseEmitter emitter, String eventName, Object data) {
        try {
            emitter.send(SseEmitter.event().name(eventName).data(data));
        } catch (IOException ignored) {
            // se falhar no "connected", não tem problema (cliente desconectou)
        }
    }
}
