package com.cemear.fila_conferencia_api.conferencia.mongo;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface PedidoConferenciaRepository extends MongoRepository<PedidoConferenciaDoc, String> {

    Optional<PedidoConferenciaDoc> findByNunota(Long nunota);

    // ✅ Busca em lote (para enriquecer os cards sem N+1)
    List<PedidoConferenciaDoc> findByNunotaIn(Iterable<Long> nunotas);
}