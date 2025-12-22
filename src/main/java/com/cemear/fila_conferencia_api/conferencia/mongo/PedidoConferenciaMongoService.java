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
    // DEFINIR CONFERENTE (já existia – mantém igual)
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
                    novo.setNunota(nunota);
                    novo.setCreatedAt(Instant.now());
                    return novo;
                });

        doc.setConferenteId(conferenteId);
        doc.setConferenteNome(conferenteNome);
        doc.setUpdatedAt(Instant.now());

        repo.save(doc);
    }

    // ============================================================
    // BUSCA EM LOTE (NÃO QUEBRA NADA)
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
}
