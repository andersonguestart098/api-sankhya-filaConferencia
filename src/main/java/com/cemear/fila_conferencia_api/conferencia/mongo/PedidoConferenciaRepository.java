package com.cemear.fila_conferencia_api.conferencia.mongo;

import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.List;
import java.util.Optional;

public interface PedidoConferenciaRepository
        extends MongoRepository<PedidoConferenciaDoc, String> {

    Optional<PedidoConferenciaDoc> findByNunota(Long nunota);

    // ✅ Busca em lote (para enriquecer os cards sem N+1)
    List<PedidoConferenciaDoc> findByNunotaIn(List<Long> nunotas);
}
