// src/main/java/com/cemear/fila_conferencia_api/conferencia/mongo/ItemConferenciaRepository.java
package com.cemear.fila_conferencia_api.conferencia.mongo;

import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.List;
import java.util.Optional;

public interface ItemConferenciaRepository extends MongoRepository<ItemConferenciaDoc, String> {

    Optional<ItemConferenciaDoc> findByNunotaAndSequencia(Long nunota, Integer sequencia);

    List<ItemConferenciaDoc> findByNunota(Long nunota);
}
