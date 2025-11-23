// src/main/java/com/cemear/fila_conferencia_api/conferencia/PedidosPendentesScheduler.java
package com.cemear.fila_conferencia_api.conferencia;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class PedidosPendentesScheduler {

    private final PedidoConferenciaService pedidoConferenciaService;

    // roda a cada 30 segundos (30000 ms)
    @Scheduled(fixedDelay = 30000)
    public void verificarPedidosPendentes() {
        try {
            // ✅ usa o método que já existe no service
            List<PedidoConferenciaDto> pendentes =
                    pedidoConferenciaService.listarPendentes();

            log.info("Scheduler - pedidos pendentes: {}", pendentes.size());

            // aqui depois você pode:
            // - detectar novos NUNOTA
            // - mandar push pro conferente, etc.
        } catch (Exception e) {
            log.error("Erro ao executar scheduler de pedidos pendentes", e);
        }
    }
}
