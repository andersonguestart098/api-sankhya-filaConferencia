package com.cemear.fila_conferencia_api.conferencia.mongo;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;

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
                    novo.setNunota(nunota);
                    novo.setCreatedAt(Instant.now());
                    return novo;
                });

        doc.setConferenteId(conferenteId);
        doc.setConferenteNome(conferenteNome);
        doc.setUpdatedAt(Instant.now());

        repo.save(doc);
    }
}
