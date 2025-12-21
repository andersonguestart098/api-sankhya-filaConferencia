package com.cemear.fila_conferencia_api.conferencia.mongo;

import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.Optional;

public interface PedidoConferenciaRepository
        extends MongoRepository<PedidoConferenciaDoc, String> {

    Optional<PedidoConferenciaDoc> findByNunota(Long nunota);
}
