package com.cemear.fila_conferencia_api.conferencia;

import com.cemear.fila_conferencia_api.conferencia.mongo.PedidoConferenciaDoc;
import com.cemear.fila_conferencia_api.conferencia.mongo.PedidoConferenciaMongoService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Service
@RequiredArgsConstructor
public class StatusConferenciaFastScheduler {

    private final PedidoConferenciaService pedidoConferenciaService;
    private final PedidoConferenciaMongoService pedidoConferenciaMongoService;
    private final PedidoConferenciaEventPublisher eventPublisher;

    private final AtomicBoolean running = new AtomicBoolean(false);

    @Scheduled(fixedDelay = 5000)
    public void verificarStatusRapido() {

        if (!running.compareAndSet(false, true)) {
            log.warn("[FAST_SCHED] skip: previous run still running");
            return;
        }

        long t0 = System.currentTimeMillis();

        int changed = 0;

        try {

            List<PedidoStatusRapidoDto> lista =
                    pedidoConferenciaService.listarStatusRapido();

            if (lista == null || lista.isEmpty()) {
                return;
            }

            List<Long> nunotas = lista.stream()
                    .map(PedidoStatusRapidoDto::nunota)
                    .filter(Objects::nonNull)
                    .distinct()
                    .toList();

            Map<Long, PedidoConferenciaDoc> mongoMap =
                    pedidoConferenciaMongoService.mapByNunota(nunotas);

            for (PedidoStatusRapidoDto atual : lista) {

                Long nunota = atual.nunota();

                if (nunota == null) {
                    continue;
                }

                String statusAtual = normalizeStatus(
                        atual.statusConferencia()
                );

                if (statusAtual == null) {
                    continue;
                }

                PedidoConferenciaDoc mongo =
                        mongoMap.get(nunota);

                String statusMongo = mongo != null
                        ? normalizeStatus(
                        mongo.getLastStatusConferencia()
                )
                        : null;

                boolean mudou =
                        statusMongo == null
                                || !statusAtual.equals(statusMongo);

                if (!mudou) {
                    continue;
                }

                changed++;

                log.info(
                        "[FAST_SCHED] mudança detectada nunota={} {} -> {}",
                        nunota,
                        statusMongo,
                        statusAtual
                );

                if ("AC".equals(statusAtual)) {
                    List<PedidoConferenciaDto> pedidoCompleto =
                            pedidoConferenciaService.listarPendentesPaginado(
                                    0,
                                    1,
                                    null,
                                    null,
                                    null,
                                    nunota
                            );

                    if (pedidoCompleto != null && !pedidoCompleto.isEmpty()) {
                        log.info("[FAST_SCHED] snapshot rápido salvo nunota={} itens={}",
                                nunota,
                                pedidoCompleto.get(0).getItens() != null ? pedidoCompleto.get(0).getItens().size() : 0
                        );

                        pedidoConferenciaMongoService.upsertSnapshot(pedidoCompleto);
                    }
                }

                pedidoConferenciaMongoService.atualizarStatusConferencia(
                        nunota,
                        statusAtual,
                        atual.nuconf()
                );

                eventPublisher.pedidoStatusChanged(
                        nunota,
                        statusAtual,
                        false
                );


            }

        } catch (Exception e) {

            log.error(
                    "[FAST_SCHED] erro: {}",
                    e.getMessage(),
                    e
            );

        } finally {

            long totalMs = System.currentTimeMillis() - t0;

            log.info(
                    "[FAST_SCHED] total={}ms changed={}",
                    totalMs,
                    changed
            );

            running.set(false);
        }
    }

    private String normalizeStatus(String status) {

        if (status == null || status.isBlank()) {
            return null;
        }

        return status.trim().toUpperCase();
    }
}