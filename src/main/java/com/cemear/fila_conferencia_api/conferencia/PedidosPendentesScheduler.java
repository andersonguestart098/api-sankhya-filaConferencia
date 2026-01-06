package com.cemear.fila_conferencia_api.conferencia;

import com.cemear.fila_conferencia_api.auth.PushService;
import com.cemear.fila_conferencia_api.auth.Usuario;
import com.cemear.fila_conferencia_api.auth.UsuarioRepository;
import com.cemear.fila_conferencia_api.conferencia.mongo.PedidoConferenciaMongoService;
import com.cemear.fila_conferencia_api.conferencia.mongo.PedidoConferenciaDoc;
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

    private Set<Long> ultimoSnapshot = new HashSet<>();

    private volatile List<Usuario> usuariosCache = List.of();
    private volatile long usuariosCacheAtMs = 0;

    private final AtomicBoolean running = new AtomicBoolean(false);

    @Scheduled(fixedDelay = 2000)
    public void verificarPedidosPendentes() {
        if (!running.compareAndSet(false, true)) {
            log.warn("[SCHED] skip: previous run still running");
            return;
        }

        long t0 = System.currentTimeMillis();

        try {
            long tList0 = System.currentTimeMillis();
            List<PedidoConferenciaDto> pendentes = pedidoConferenciaService.listarPendentes();
            long tList1 = System.currentTimeMillis();

            int total = (pendentes == null) ? 0 : pendentes.size();
            log.info("[SCHED] listarPendentes={}ms total={}", (tList1 - tList0), total);

            if (pendentes == null || pendentes.isEmpty()) {
                return;
            }

            // ============================================================
            // 1) PUSH: detectar "novos pedidos" (AC/R) com snapshot local
            // ============================================================
            long tPush0 = System.currentTimeMillis();

            Set<Long> atualAcR = new HashSet<>();
            for (PedidoConferenciaDto p : pendentes) {
                Long nunota = p.getNunota();
                String status = p.getStatusConferencia();

                if (nunota != null && ("AC".equals(status) || "R".equals(status))) {
                    atualAcR.add(nunota);
                }
            }

            Set<Long> novos = new HashSet<>(atualAcR);
            novos.removeAll(ultimoSnapshot);

            if (!novos.isEmpty()) {
                log.info("Novos pedidos (STATUS AC/R) encontrados: {}", novos);

                List<Usuario> usuarios = getUsuariosComCache();

                for (Long nunota : novos) {
                    String titulo = "Novo pedido na fila";
                    String corpo  = "Pedido " + nunota + " aguardando conferência";

                    for (Usuario u : usuarios) {
                        String token = u.getPushToken();
                        if (token != null && !token.isBlank()) {
                            pushService.enviarPush(token, titulo, corpo);
                        }
                    }
                }
            }

            ultimoSnapshot = atualAcR;

            long tPush1 = System.currentTimeMillis();
            log.info("[SCHED] pushBlock={}ms novos={}", (tPush1 - tPush0), novos.size());

            // ============================================================
            // 2) SSE + Mongo: detectar mudança de status por NUNOTA
            // ============================================================

            long tMongo0 = System.currentTimeMillis();

            List<Long> nunotas = pendentes.stream()
                    .map(PedidoConferenciaDto::getNunota)
                    .filter(Objects::nonNull)
                    .distinct()
                    .collect(Collectors.toList());

            if (nunotas.isEmpty()) return;

            Map<Long, PedidoConferenciaDoc> mongoMap =
                    pedidoConferenciaMongoService.mapByNunota(nunotas);

            long tMongo1 = System.currentTimeMillis();
            log.info("[SCHED] mongoMapByNunota={}ms nunotas={}", (tMongo1 - tMongo0), nunotas.size());

            long tLoop0 = System.currentTimeMillis();
            int changed = 0;
            int initial = 0;

            for (PedidoConferenciaDto p : pendentes) {
                Long nunota = p.getNunota();
                if (nunota == null) continue;

                String statusAtual = p.getStatusConferencia();
                if (statusAtual == null || statusAtual.isBlank()) continue;

                PedidoConferenciaDoc doc = mongoMap.get(nunota);
                String statusAnterior = (doc != null) ? doc.getLastStatusConferencia() : null;

                // ✅ primeiro status visto: grava e EMITE (resolve “perdi A”)
                if (statusAnterior == null) {
                    pedidoConferenciaMongoService.upsertLastStatusConferencia(nunota, statusAtual);

                    Map<String, Object> payload = Map.of(
                            "nunota", nunota,
                            "statusConferencia", statusAtual,
                            "updatedAt", Instant.now().toString(),
                            "initial", true
                    );

                    initial++;
                    log.info("STATUS_INITIAL nunota={} status={}", nunota, statusAtual);
                    sseHub.broadcast("pedido_status_changed", payload);
                    continue;
                }

                if (!statusAtual.equals(statusAnterior)) {
                    pedidoConferenciaMongoService.upsertLastStatusConferencia(nunota, statusAtual);

                    Map<String, Object> payload = Map.of(
                            "nunota", nunota,
                            "statusConferencia", statusAtual,
                            "updatedAt", Instant.now().toString()
                    );

                    changed++;
                    log.info("STATUS_CHANGED nunota={} {} -> {}", nunota, statusAnterior, statusAtual);
                    sseHub.broadcast("pedido_status_changed", payload);
                }
            }

            long tLoop1 = System.currentTimeMillis();
            log.info("[SCHED] loopEmit={}ms changed={} initial={}", (tLoop1 - tLoop0), changed, initial);

        } catch (Exception e) {
            log.error("Erro no scheduler de pedidos pendentes: {}", e.getMessage(), e);
        } finally {
            long t1 = System.currentTimeMillis();
            log.info("[SCHED] total={}ms", (t1 - t0));
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
}
