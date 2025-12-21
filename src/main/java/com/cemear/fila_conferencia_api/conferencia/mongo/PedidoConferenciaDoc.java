package com.cemear.fila_conferencia_api.conferencia.mongo;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;

@Data
@Document(collection = "pedido_conferencia")
public class PedidoConferenciaDoc {

    @Id
    private String id;

    private Long nunota;

    private Integer conferenteId;
    private String conferenteNome;

    private Instant createdAt;
    private Instant updatedAt;
}
