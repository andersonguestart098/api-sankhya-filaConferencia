// src/main/java/com/cemear/fila_conferencia_api/conferencia/mongo/ItemConferenciaRepository.java
package com.cemear.fila_conferencia_api.conferencia.mongo;

import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.List;

public interface ItemConferenciaRepository extends MongoRepository<ItemConferenciaDoc, String> {

    /**
     * IMPORTANTE:
     * - Usamos List aqui porque existem casos no banco com duplicidade (nunota+sequencia).
     * - O service escolhe o "melhor" (mais recente) e evita IncorrectResultSizeDataAccessException.
     * - Depois que você limpar duplicados e criar índice único, você pode voltar a Optional.
     */
    List<ItemConferenciaDoc> findAllByNunotaAndSequencia(Long nunota, Integer sequencia);

    List<ItemConferenciaDoc> findByNunota(Long nunota);
}
