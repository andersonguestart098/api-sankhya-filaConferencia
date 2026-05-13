package com.cemear.fila_conferencia_api.conferencia.mongo;

import com.cemear.fila_conferencia_api.conferencia.PedidoConferenciaDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
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

    public Optional<PedidoConferenciaDoc> buscarPorNunota(Long nunota) {
        if (nunota == null) {
            return Optional.empty();
        }
        return repo.findByNunota(nunota);
    }

    public boolean possuiConferenteDefinido(Long nunota) {
        return buscarPorNunota(nunota)
                .map(doc ->
                        doc.getConferenteId() != null &&
                                doc.getConferenteNome() != null &&
                                !doc.getConferenteNome().trim().isEmpty()
                )
                .orElse(false);
    }

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

    public void upsertSnapshot(List<PedidoConferenciaDto> pedidos) {
        if (pedidos == null || pedidos.isEmpty()) {
            return;
        }

        Instant now = Instant.now();

        for (PedidoConferenciaDto pedido : pedidos) {
            if (pedido == null || pedido.getNunota() == null) {
                continue;
            }

            Long nunota = pedido.getNunota();

            PedidoConferenciaDoc doc = repo.findByNunota(nunota)
                    .orElseGet(() -> {
                        PedidoConferenciaDoc novo = new PedidoConferenciaDoc();
                        novo.setNunota(nunota);
                        novo.setCreatedAt(now);
                        return novo;
                    });

            String statusAtual = normalizeStatus(pedido.getStatusConferencia());
            String statusAnterior = normalizeStatus(doc.getLastStatusConferencia());

            doc.setSnapshot(pedido);

            if (statusAtual != null) {
                doc.setLastStatusConferencia(statusAtual);
                doc.setLastStatusUpdatedAt(now);
            }

            if ("AC".equals(statusAtual) && doc.getConferenciaIniciadaAt() == null) {
                doc.setConferenciaIniciadaAt(now);
            }

            if ("F".equals(statusAtual) && doc.getConferenciaFinalizadaAt() == null) {
                doc.setConferenciaFinalizadaAt(now);

                Instant inicio = doc.getConferenciaIniciadaAt();
                if (inicio != null) {
                    long tempoMs = doc.getConferenciaFinalizadaAt().toEpochMilli() - inicio.toEpochMilli();
                    doc.setTempoConferenciaMs(Math.max(tempoMs, 0L));
                }
            }

            if (statusAnterior == null && statusAtual != null) {
                doc.setLastStatusConferencia(statusAtual);
                doc.setLastStatusUpdatedAt(now);
            }

            doc.setUpdatedAt(now);
            repo.save(doc);
        }
    }

    public void insertNovosSnapshots(List<PedidoConferenciaDto> novos) {
        if (novos == null || novos.isEmpty()) {
            return;
        }

        Instant now = Instant.now();
        List<PedidoConferenciaDoc> docs = new ArrayList<>();

        for (PedidoConferenciaDto pedido : novos) {
            if (pedido == null || pedido.getNunota() == null) {
                continue;
            }

            PedidoConferenciaDoc doc = new PedidoConferenciaDoc();
            doc.setNunota(pedido.getNunota());
            doc.setCreatedAt(now);
            doc.setUpdatedAt(now);
            doc.setSnapshot(pedido);

            String statusAtual = normalizeStatus(pedido.getStatusConferencia());

            if (statusAtual != null) {
                doc.setLastStatusConferencia(statusAtual);
                doc.setLastStatusUpdatedAt(now);
            }

            if ("AC".equals(statusAtual)) {
                doc.setConferenciaIniciadaAt(now);
            }

            if ("F".equals(statusAtual)) {
                doc.setConferenciaFinalizadaAt(now);
            }

            docs.add(doc);
        }

        if (!docs.isEmpty()) {
            repo.saveAll(docs);
            log.info("[MONGO] insertNovosSnapshots={}", docs.size());
        }
    }

    public PedidoConferenciaDoc save(PedidoConferenciaDoc doc) {
        return repo.save(doc);
    }

    public List<PedidoConferenciaDoc> findAllSnapshot() {
        return repo.findAll();
    }

    public String getLastStatusConferencia(Long nunota) {
        return repo.findByNunota(nunota)
                .map(PedidoConferenciaDoc::getLastStatusConferencia)
                .orElse(null);
    }

    public PedidoConferenciaDoc atualizarStatusConferencia(
            Long nunota,
            String status,
            Long nuconf
    ) {
        Instant now = Instant.now();

        PedidoConferenciaDoc doc = repo.findByNunota(nunota)
                .orElseGet(() -> {
                    PedidoConferenciaDoc novo = new PedidoConferenciaDoc();
                    novo.setNunota(nunota);
                    novo.setCreatedAt(now);
                    novo.setUpdatedAt(now);
                    return novo;
                });

        String statusNormalizado = normalizeStatus(status);

        doc.setLastStatusConferencia(statusNormalizado);
        doc.setLastStatusUpdatedAt(now);

        if (nuconf != null) {
            doc.setNuconf(nuconf);
        }

        if ("AC".equals(statusNormalizado)) {
            if (doc.getConferenciaIniciadaAt() == null) {
                doc.setConferenciaIniciadaAt(now);
            }
        }

        if ("F".equals(statusNormalizado)) {

            if (doc.getConferenciaFinalizadaAt() == null) {
                doc.setConferenciaFinalizadaAt(now);
            }

            Instant inicio = doc.getConferenciaIniciadaAt();

            if (inicio != null) {
                long tempoMs =
                        doc.getConferenciaFinalizadaAt().toEpochMilli()
                                - inicio.toEpochMilli();

                doc.setTempoConferenciaMs(
                        Math.max(tempoMs, 0L)
                );
            }
        }

        PedidoConferenciaDto snapshot = doc.getSnapshot();

        if (snapshot != null) {
            snapshot.setStatusConferencia(statusNormalizado);

            if (nuconf != null) {
                snapshot.setNuconf(nuconf);
            }

            if ("F".equals(statusNormalizado)) {
                snapshot.setTempoConferenciaMs(doc.getTempoConferenciaMs());
            }

            doc.setSnapshot(snapshot);
        }


        doc.setUpdatedAt(now);

        return repo.save(doc);
    }

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

        String statusNormalizado = normalizeStatus(status);

        doc.setLastStatusConferencia(statusNormalizado);
        doc.setLastStatusUpdatedAt(now);

        if ("AC".equals(statusNormalizado) && doc.getConferenciaIniciadaAt() == null) {
            doc.setConferenciaIniciadaAt(now);
        }

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

        if (doc.getConferenciaIniciadaAt() == null) {
            doc.setConferenciaIniciadaAt(
                    doc.getCreatedAt() != null ? doc.getCreatedAt() : now
            );
        }

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



    private String normalizeStatus(String status) {
        if (status == null || status.isBlank()) {
            return null;
        }
        return status.trim().toUpperCase();
    }
}