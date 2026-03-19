package com.cemear.fila_conferencia_api.conferencia.mongo;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class PedidoConferenciaMongoService {

    private final PedidoConferenciaRepository repo;

    // ============================================================
    // DEFINIR CONFERENTE
    // ============================================================
    public void definirConferente(
            Long nunota,
            Integer conferenteId,
            String conferenteNome
    ) {
        log.info(
                "DEFINIR_CONFERENTE nunota={} conferenteId={} conferenteNome={}",
                nunota, conferenteId, conferenteNome
        );

        PedidoConferenciaDoc doc = repo
                .findByNunota(nunota)
                .orElseGet(() -> {
                    PedidoConferenciaDoc novo = new PedidoConferenciaDoc();
                    Instant now = Instant.now();
                    novo.setNunota(nunota);
                    novo.setCreatedAt(now);
                    novo.setUpdatedAt(now);
                    return novo;
                });

        doc.setConferenteId(conferenteId);
        doc.setConferenteNome(conferenteNome);
        doc.setUpdatedAt(Instant.now());

        repo.save(doc);
    }

    // ============================================================
    // BUSCAR POR NUNOTA
    // ============================================================
    public Optional<PedidoConferenciaDoc> buscarPorNunota(Long nunota) {
        if (nunota == null) {
            return Optional.empty();
        }
        return repo.findByNunota(nunota);
    }

    // ============================================================
    // POSSUI CONFERENTE DEFINIDO?
    // ============================================================
    public boolean possuiConferenteDefinido(Long nunota) {
        return buscarPorNunota(nunota)
                .map(doc ->
                        doc.getConferenteId() != null &&
                                doc.getConferenteNome() != null &&
                                !doc.getConferenteNome().trim().isEmpty()
                )
                .orElse(false);
    }

    // ============================================================
    // BUSCA EM LOTE
    // ============================================================
    public Map<Long, PedidoConferenciaDoc> mapByNunota(List<Long> nunotas) {
        if (nunotas == null || nunotas.isEmpty()) {
            return Map.of();
        }

        return repo.findByNunotaIn(nunotas)
                .stream()
                .collect(Collectors.toMap(
                        PedidoConferenciaDoc::getNunota,
                        Function.identity(),
                        (a, b) -> a
                ));
    }

    // ============================================================
    // STATUS: pegar último status salvo
    // ============================================================
    public String getLastStatusConferencia(Long nunota) {
        return repo.findByNunota(nunota)
                .map(PedidoConferenciaDoc::getLastStatusConferencia)
                .orElse(null);
    }

    // ============================================================
    // STATUS + TEMPO DA CONFERÊNCIA (AC -> F)
    // ============================================================
    public PedidoConferenciaDoc upsertLastStatusConferencia(Long nunota, String status) {
        Instant now = Instant.now();

        PedidoConferenciaDoc doc = repo.findByNunota(nunota)
                .orElseGet(() -> {
                    PedidoConferenciaDoc novo = new PedidoConferenciaDoc();
                    novo.setNunota(nunota);
                    novo.setCreatedAt(now);
                    novo.setUpdatedAt(now);
                    return novo;
                });

        String statusNormalizado = status == null ? null : status.trim().toUpperCase();
        String statusAnterior = doc.getLastStatusConferencia() == null
                ? null
                : doc.getLastStatusConferencia().trim().toUpperCase();

        doc.setLastStatusConferencia(statusNormalizado);
        doc.setLastStatusUpdatedAt(now);

        // início: grava só a primeira vez que entra em AC
        if ("AC".equals(statusNormalizado) && doc.getConferenciaIniciadaAt() == null) {
            doc.setConferenciaIniciadaAt(now);
        }

        // final: grava só a primeira vez que entra em F
        if ("F".equals(statusNormalizado) && doc.getConferenciaFinalizadaAt() == null) {
            doc.setConferenciaFinalizadaAt(now);

            Instant inicio = doc.getConferenciaIniciadaAt();
            if (inicio != null) {
                long tempoMs = doc.getConferenciaFinalizadaAt().toEpochMilli() - inicio.toEpochMilli();
                doc.setTempoConferenciaMs(Math.max(tempoMs, 0L));
            }
        }

        doc.setUpdatedAt(now);

        return repo.save(doc);
    }

    // ============================================================
    // MARCAR INÍCIO MANUAL (opcional)
    // ============================================================
    public PedidoConferenciaDoc marcarInicioConferencia(Long nunota) {
        Instant now = Instant.now();

        PedidoConferenciaDoc doc = repo.findByNunota(nunota)
                .orElseGet(() -> {
                    PedidoConferenciaDoc novo = new PedidoConferenciaDoc();
                    novo.setNunota(nunota);
                    novo.setCreatedAt(now);
                    novo.setUpdatedAt(now);
                    return novo;
                });

        if (doc.getConferenciaIniciadaAt() == null) {
            doc.setConferenciaIniciadaAt(now);
        }

        doc.setUpdatedAt(now);
        return repo.save(doc);
    }

    // ============================================================
// MARCAR FINALIZAÇÃO MANUAL (opcional)
// ============================================================
    public PedidoConferenciaDoc marcarFinalizacaoConferencia(Long nunota) {
        Instant now = Instant.now();

        PedidoConferenciaDoc doc = repo.findByNunota(nunota)
                .orElseGet(() -> {
                    PedidoConferenciaDoc novo = new PedidoConferenciaDoc();
                    novo.setNunota(nunota);
                    novo.setCreatedAt(now);
                    novo.setUpdatedAt(now);
                    return novo;
                });

        // fallback: se o polling não pegou o AC, cria um início mínimo
        if (doc.getConferenciaIniciadaAt() == null) {
            doc.setConferenciaIniciadaAt(
                    doc.getCreatedAt() != null ? doc.getCreatedAt() : now
            );
        }

        // grava fim só uma vez
        if (doc.getConferenciaFinalizadaAt() == null) {
            doc.setConferenciaFinalizadaAt(now);
        }

        Instant inicio = doc.getConferenciaIniciadaAt();
        Instant fim = doc.getConferenciaFinalizadaAt();

        if (inicio != null && fim != null) {
            long tempoMs = fim.toEpochMilli() - inicio.toEpochMilli();
            doc.setTempoConferenciaMs(Math.max(tempoMs, 0L));
        }

        doc.setLastStatusConferencia("F");
        doc.setLastStatusUpdatedAt(now);
        doc.setUpdatedAt(now);

        return repo.save(doc);
    }
}