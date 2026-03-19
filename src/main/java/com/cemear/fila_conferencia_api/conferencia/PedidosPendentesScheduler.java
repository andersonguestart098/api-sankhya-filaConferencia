package com.cemear.fila_conferencia_api.conferencia;

import com.cemear.fila_conferencia_api.auth.PushService;
import com.cemear.fila_conferencia_api.auth.Usuario;
import com.cemear.fila_conferencia_api.auth.UsuarioRepository;
import com.cemear.fila_conferencia_api.conferencia.mongo.PedidoConferenciaDoc;
import com.cemear.fila_conferencia_api.conferencia.mongo.PedidoConferenciaMongoService;
import com.cemear.fila_conferencia_api.conferencia.sse.SseHub;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class PedidosPendentesScheduler {

    private final PedidoConferenciaService pedidoConferenciaService;
    private final PushService pushService;
    private final UsuarioRepository usuarioRepository;
    private final PedidoConferenciaMongoService pedidoConferenciaMongoService;
    private final SseHub sseHub;

    private Set<Long> ultimoSnapshotAcR = new HashSet<>();
    private volatile List<Usuario> usuariosCache = List.of();
    private volatile long usuariosCacheAtMs = 0;

    private final AtomicBoolean running = new AtomicBoolean(false);

    private static final boolean ENABLE_PUSH = false;

    @Scheduled(fixedDelay = 10000)
    public void verificarPedidosPendentes() {
        if (!running.compareAndSet(false, true)) {
            log.warn("[SCHED] skip: previous run still running");
            return;
        }

        final long t0 = System.currentTimeMillis();

        int total = 0;
        int changed = 0;
        int initial = 0;
        int novos = 0;

        try {
            final long tList0 = System.currentTimeMillis();
            List<PedidoConferenciaDto> pendentes = pedidoConferenciaService.listarPendentes();
            final long tListMs = System.currentTimeMillis() - tList0;

            total = (pendentes == null) ? 0 : pendentes.size();
            log.info("[SCHED] listarPendentes={}ms total={}", tListMs, total);

            if (pendentes == null || pendentes.isEmpty()) {
                return;
            }

            if (ENABLE_PUSH) {
                final long tPush0 = System.currentTimeMillis();

                Set<Long> atualAcR = new HashSet<>();
                for (PedidoConferenciaDto p : pendentes) {
                    Long nunota = p.getNunota();
                    String status = normalizeStatus(p.getStatusConferencia());
                    if (nunota != null && ("AC".equals(status) || "R".equals(status))) {
                        atualAcR.add(nunota);
                    }
                }

                Set<Long> novosSet = new HashSet<>(atualAcR);
                novosSet.removeAll(ultimoSnapshotAcR);
                novos = novosSet.size();

                ultimoSnapshotAcR = atualAcR;

                if (!novosSet.isEmpty()) {
                    List<Usuario> usuarios = getUsuariosComCache();

                    for (Long nunota : novosSet) {
                        String titulo = "Novo pedido na fila";
                        String corpo = "Pedido " + nunota + " aguardando conferência";

                        for (Usuario u : usuarios) {
                            String token = u.getPushToken();
                            if (token == null || token.isBlank()) continue;

                            try {
                                pushService.enviarPush(token, titulo, corpo);
                            } catch (Exception ex) {
                                log.warn("[SCHED] push failed token={} nunota={} err={}",
                                        token.substring(0, Math.min(8, token.length())),
                                        nunota,
                                        ex.getMessage());
                            }
                        }
                    }
                }

                final long tPushMs = System.currentTimeMillis() - tPush0;
                log.info("[SCHED] pushBlock={}ms novos={}", tPushMs, novos);
            }

            final List<Long> nunotas = pendentes.stream()
                    .map(PedidoConferenciaDto::getNunota)
                    .filter(Objects::nonNull)
                    .distinct()
                    .collect(Collectors.toList());

            if (nunotas.isEmpty()) {
                return;
            }

            final long tMongo0 = System.currentTimeMillis();
            Map<Long, PedidoConferenciaDoc> mongoMapAnterior = pedidoConferenciaMongoService.mapByNunota(nunotas);
            final long tMongoMs = System.currentTimeMillis() - tMongo0;
            log.info("[SCHED] mongoMapByNunota={}ms nunotas={}", tMongoMs, nunotas.size());

            final long tLoop0 = System.currentTimeMillis();

            for (PedidoConferenciaDto p : pendentes) {
                Long nunota = p.getNunota();
                if (nunota == null) continue;

                String statusAtual = normalizeStatus(p.getStatusConferencia());
                if (statusAtual == null || statusAtual.isBlank()) continue;

                PedidoConferenciaDoc docAnterior = mongoMapAnterior.get(nunota);
                String statusAnterior = docAnterior != null
                        ? normalizeStatus(docAnterior.getLastStatusConferencia())
                        : null;

                if (statusAnterior == null) {
                    Map<String, Object> payload = Map.of(
                            "nunota", nunota,
                            "statusConferencia", statusAtual,
                            "updatedAt", Instant.now().toString(),
                            "initial", true
                    );

                    initial++;
                    sseHub.broadcast("pedido_status_changed", payload);
                    continue;
                }

                if (!statusAtual.equals(statusAnterior)) {
                    Map<String, Object> payload = Map.of(
                            "nunota", nunota,
                            "statusConferencia", statusAtual,
                            "updatedAt", Instant.now().toString()
                    );

                    changed++;
                    sseHub.broadcast("pedido_status_changed", payload);
                }
            }

            final long tLoopMs = System.currentTimeMillis() - tLoop0;
            log.info("[SCHED] loopEmit={}ms changed={} initial={}", tLoopMs, changed, initial);

            final long tSave0 = System.currentTimeMillis();
            pedidoConferenciaMongoService.upsertSnapshot(pendentes);
            final long tSaveMs = System.currentTimeMillis() - tSave0;
            log.info("[SCHED] saveSnapshot={}ms total={}", tSaveMs, pendentes.size());

        } catch (Exception e) {
            log.error("[SCHED] error: {}", e.getMessage(), e);
        } finally {
            final long totalMs = System.currentTimeMillis() - t0;
            log.info("[SCHED] total={}ms totalPendentes={} changed={} initial={} novos={}",
                    totalMs, total, changed, initial, novos);
            running.set(false);
        }
    }

    private List<Usuario> getUsuariosComCache() {
        long now = System.currentTimeMillis();
        if ((now - usuariosCacheAtMs) < 60_000 && usuariosCache != null && !usuariosCache.isEmpty()) {
            return usuariosCache;
        }
        List<Usuario> usuarios = usuarioRepository.findAll();
        usuariosCache = usuarios;
        usuariosCacheAtMs = now;
        return usuarios;
    }

    private String normalizeStatus(String status) {
        if (status == null || status.isBlank()) {
            return null;
        }
        return status.trim().toUpperCase();
    }
}