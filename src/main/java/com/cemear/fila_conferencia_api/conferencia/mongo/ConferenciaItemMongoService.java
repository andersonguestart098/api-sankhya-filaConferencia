// src/main/java/com/cemear/fila_conferencia_api/conferencia/mongo/ConferenciaItemMongoService.java
package com.cemear.fila_conferencia_api.conferencia.mongo;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

@Service
@RequiredArgsConstructor
public class ConferenciaItemMongoService {

    private final ItemConferenciaRepository repo;

    /**
     * Seleciona o "melhor" documento quando houver duplicidade (mesmo nunota + sequencia).
     * Estratégia: pega o mais recentemente atualizado (updatedAt). Se updatedAt for nulo, cai pro createdAt.
     */
    private Optional<ItemConferenciaDoc> buscarMelhor(Long nunota, Integer sequencia) {
        List<ItemConferenciaDoc> docs = repo.findAllByNunotaAndSequencia(nunota, sequencia);

        return docs.stream()
                .max(Comparator.comparing(
                        d -> d.getUpdatedAt() != null ? d.getUpdatedAt() : d.getCreatedAt(),
                        Comparator.nullsLast(Comparator.naturalOrder())
                ));
    }

    private ItemConferenciaDoc upsertBase(Long nunota,
                                          Integer sequencia,
                                          Integer codProd,
                                          Integer codUsuario) {

        ItemConferenciaDoc doc = buscarMelhor(nunota, sequencia)
                .orElseGet(() -> {
                    ItemConferenciaDoc novo = new ItemConferenciaDoc();
                    novo.setNunota(nunota);
                    novo.setSequencia(sequencia);
                    novo.setCreatedAt(Instant.now());
                    return novo;
                });

        // Atualizações comuns
        doc.setUpdatedAt(Instant.now());
        if (codProd != null) doc.setCodProd(codProd);
        if (codUsuario != null) doc.setCodUsuario(codUsuario);

        return doc;
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

    /**
     * Busca por nunota+sequencia tolerando duplicidade no Mongo.
     * Retorna o mais recente (updatedAt/createdAt).
     *
     * IMPORTANTE:
     * - Para isso funcionar, o repositório deve ter:
     *   List<ItemConferenciaDoc> findAllByNunotaAndSequencia(Long nunota, Integer sequencia);
     * - O ideal depois é limpar duplicados e criar índice único (nunota+sequencia).
     */
    public Optional<ItemConferenciaDoc> buscar(Long nunota, Integer sequencia) {
        return buscarMelhor(nunota, sequencia);
    }
}
