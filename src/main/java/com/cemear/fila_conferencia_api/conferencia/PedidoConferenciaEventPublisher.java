package com.cemear.fila_conferencia_api.conferencia;

import com.cemear.fila_conferencia_api.conferencia.sse.SseHub;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class PedidoConferenciaEventPublisher {

    private final SseHub sseHub;

    public void pedidoStatusChanged(Long nunota, String statusConferencia, boolean initial) {
        Map<String, Object> payload = Map.of(
                "nunota", nunota,
                "statusConferencia", statusConferencia,
                "updatedAt", Instant.now().toString(),
                "initial", initial
        );

        log.info("📡 [SSE_PUBLISH] event=pedido_status_changed nunota={} status={} initial={} clients={}",
                nunota,
                statusConferencia,
                initial,
                sseHub.countClients()
        );

        sseHub.broadcast("pedido_status_changed", payload);
    }

    public void pedidoFinalizado(Long nunota, Long nuconf) {
        Map<String, Object> payload = Map.of(
                "nunota", nunota,
                "nuconf", nuconf,
                "statusConferencia", "F",
                "updatedAt", Instant.now().toString()
        );

        log.info("📡 [SSE_PUBLISH] event=pedido_finalizado nunota={} nuconf={} clients={}",
                nunota,
                nuconf,
                sseHub.countClients()
        );

        sseHub.broadcast("pedido_finalizado", payload);
    }
}