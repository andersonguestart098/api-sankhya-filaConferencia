package com.cemear.fila_conferencia_api.conferencia.mongo;

import com.cemear.fila_conferencia_api.conferencia.PedidoConferenciaDto;
import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;

@Data
@Document(collection = "pedido_conferencia")
public class PedidoConferenciaDoc {

    @Id
    private String id;

    @Indexed(unique = true)
    private Long nunota;

    private Integer conferenteId;
    private String conferenteNome;

    private Instant createdAt;
    private Instant updatedAt;

    private String lastStatusConferencia;
    private Instant lastStatusUpdatedAt;

    private Instant conferenciaIniciadaAt;
    private Instant conferenciaFinalizadaAt;
    private Long tempoConferenciaMs;

    private Long nuconf;


    // snapshot completo da fila
    private PedidoConferenciaDto snapshot;
}