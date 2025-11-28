// src/main/java/com/cemear/fila_conferencia_api/conferencia/mongo/ConferenciaItemMongoService.java
package com.cemear.fila_conferencia_api.conferencia.mongo;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Optional;

@Service
@RequiredArgsConstructor
public class ConferenciaItemMongoService {

    private final ItemConferenciaRepository repo;

    private ItemConferenciaDoc upsertBase(Long nunota,
                                          Integer sequencia,
                                          Integer codProd,
                                          Integer codUsuario) {

        return repo.findByNunotaAndSequencia(nunota, sequencia)
                .map(doc -> {
                    doc.setUpdatedAt(Instant.now());
                    if (codProd != null) doc.setCodProd(codProd);
                    if (codUsuario != null) doc.setCodUsuario(codUsuario);
                    return doc;
                })
                .orElseGet(() -> {
                    ItemConferenciaDoc doc = new ItemConferenciaDoc();
                    doc.setNunota(nunota);
                    doc.setSequencia(sequencia);
                    doc.setCodProd(codProd);
                    doc.setCodUsuario(codUsuario);
                    doc.setCreatedAt(Instant.now());
                    doc.setUpdatedAt(Instant.now());
                    return doc;
                });
    }

    // 1) Snapshot inicial: QTD ORIGINAL
    public ItemConferenciaDoc registrarQtdOriginal(Long nunota,
                                                   Integer sequencia,
                                                   Integer codProd,
                                                   Double qtdOriginal,
                                                   Integer codUsuario) {

        ItemConferenciaDoc doc = upsertBase(nunota, sequencia, codProd, codUsuario);

        // não sobrescreve se já tiver original salvo
        if (doc.getQtdOriginal() == null && qtdOriginal != null) {
            doc.setQtdOriginal(qtdOriginal);
        }
        doc.setUpdatedAt(Instant.now());
        return repo.save(doc);
    }

    // 2) Conferente digitou quantidade
    public ItemConferenciaDoc registrarQtdConferida(Long nunota,
                                                    Integer sequencia,
                                                    Double qtdConferida,
                                                    Integer codUsuario) {

        ItemConferenciaDoc doc = upsertBase(nunota, sequencia, null, codUsuario);
        doc.setQtdConferida(qtdConferida);
        doc.setUpdatedAt(Instant.now());
        return repo.save(doc);
    }

    // 3) Depois que o Sankhya cortar o item
    public ItemConferenciaDoc registrarQtdAjustada(Long nunota,
                                                   Integer sequencia,
                                                   Double qtdAjustada,
                                                   Integer codUsuario) {

        ItemConferenciaDoc doc = upsertBase(nunota, sequencia, null, codUsuario);
        doc.setQtdAjustada(qtdAjustada);
        doc.setUpdatedAt(Instant.now());
        return repo.save(doc);
    }

    public Optional<ItemConferenciaDoc> buscar(Long nunota, Integer sequencia) {
        return repo.findByNunotaAndSequencia(nunota, sequencia);
    }
}
