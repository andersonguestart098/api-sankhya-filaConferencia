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
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class PedidosPendentesScheduler {

    private final PedidoConferenciaService pedidoConferenciaService;

    // Push (mantém sua feature)
    private final PushService pushService;
    private final UsuarioRepository usuarioRepository;

    // ✅ Mongo (persistir último status)
    private final PedidoConferenciaMongoService pedidoConferenciaMongoService;

    // ✅ SSE
    private final SseHub sseHub;

    // snapshot local das NUNOTAS já vistas (somente status AC/R) -> serve pro push "novo pedido"
    private Set<Long> ultimoSnapshot = new HashSet<>();

    // cache simples p/ não buscar usuários toda execução (reduz custo)
    private volatile List<Usuario> usuariosCache = List.of();
    private volatile long usuariosCacheAtMs = 0;

    // ============================================================
    // Scheduler principal (quase real-time)
    // ============================================================
    @Scheduled(fixedDelay = 2000) // ✅ 2s (ajuste se quiser 3000/5000)
    public void verificarPedidosPendentes() {
        try {
            List<PedidoConferenciaDto> pendentes = pedidoConferenciaService.listarPendentes();

            if (pendentes == null || pendentes.isEmpty()) {
                // ainda assim não dá erro; não emite nada
                return;
            }

            // ============================================================
            // 1) PUSH: detectar "novos pedidos" (AC/R) com snapshot local
            // ============================================================
            Set<Long> atualAcR = new HashSet<>();
            for (PedidoConferenciaDto p : pendentes) {
                Long nunota = p.getNunota();
                String status = p.getStatusConferencia();

                if (nunota != null && ( "AC".equals(status) || "R".equals(status) )) {
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

            // atualiza snapshot
            ultimoSnapshot = atualAcR;

            // ============================================================
            // 2) SSE + Mongo: detectar mudança de status por NUNOTA
            //    - faz lookup em lote no Mongo (evita N+1)
            // ============================================================

            List<Long> nunotas = pendentes.stream()
                    .map(PedidoConferenciaDto::getNunota)
                    .filter(Objects::nonNull)
                    .distinct()
                    .collect(Collectors.toList());

            if (nunotas.isEmpty()) return;

            Map<Long, PedidoConferenciaDoc> mongoMap =
                    pedidoConferenciaMongoService.mapByNunota(nunotas);

            for (PedidoConferenciaDto p : pendentes) {
                Long nunota = p.getNunota();
                if (nunota == null) continue;

                String statusAtual = p.getStatusConferencia();
                if (statusAtual == null || statusAtual.isBlank()) continue;

                PedidoConferenciaDoc doc = mongoMap.get(nunota);
                String statusAnterior = (doc != null) ? doc.getLastStatusConferencia() : null;

                // Primeira vez: grava no Mongo (warmup) e não emite (evita flood no start)
                if (statusAnterior == null) {
                    pedidoConferenciaMongoService.upsertLastStatusConferencia(nunota, statusAtual);
                    continue;
                }

                // Mudou status: persiste e emite evento SSE
                if (!statusAtual.equals(statusAnterior)) {
                    pedidoConferenciaMongoService.upsertLastStatusConferencia(nunota, statusAtual);

                    // payload enxuto (o app pode atualizar local pelo nunota)
                    Map<String, Object> payload = Map.of(
                            "nunota", nunota,
                            "statusConferencia", statusAtual,
                            "updatedAt", Instant.now().toString()
                    );

                    log.info("STATUS_CHANGED nunota={} {} -> {}", nunota, statusAnterior, statusAtual);
                    sseHub.broadcast("pedido_status_changed", payload);
                }
            }

        } catch (Exception e) {
            log.error("Erro no scheduler de pedidos pendentes: {}", e.getMessage(), e);
        }
    }

    // ============================================================
    // Cache simples de usuários p/ reduzir custo no scheduler
    // ============================================================
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
