package com.cemear.fila_conferencia_api.conferencia.mongo;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class PedidoConferenciaMongoService {

    private final PedidoConferenciaRepository repo;

    // ============================================================
    // DEFINIR CONFERENTE (mantém igual)
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
    // BUSCA EM LOTE (mantém igual)
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
    // ✅ STATUS: pegar último status salvo
    // ============================================================
    public String getLastStatusConferencia(Long nunota) {
        return repo.findByNunota(nunota)
                .map(PedidoConferenciaDoc::getLastStatusConferencia)
                .orElse(null);
    }

    // ============================================================
    // ✅ STATUS: upsert do último status salvo
    // ============================================================
    public PedidoConferenciaDoc upsertLastStatusConferencia(Long nunota, String status) {
        Instant now = Instant.now();

        PedidoConferenciaDoc doc = repo.findByNunota(nunota)
                .orElseGet(() -> {
                    PedidoConferenciaDoc novo = new PedidoConferenciaDoc();
                    novo.setNunota(nunota);
                    novo.setCreatedAt(now);
                    return novo;
                });

        doc.setLastStatusConferencia(status);
        doc.setLastStatusUpdatedAt(now);
        doc.setUpdatedAt(now);

        return repo.save(doc);
    }
}
