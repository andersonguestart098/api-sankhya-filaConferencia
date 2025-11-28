// src/main/java/com/cemear/fila_conferencia_api/conferencia/mongo/ItemConferenciaDoc.java
package com.cemear.fila_conferencia_api.conferencia.mongo;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;

@Data
@Document("itens_conferencia")
public class ItemConferenciaDoc {

    @Id
    private String id;

    private Long nunota;
    private Integer sequencia;
    private Integer codProd;

    // Quantidade original da nota (QTDNEG da TGFITE na primeira vez)
    private Double qtdOriginal;

    // Quantidade que o conferente digitou
    private Double qtdConferida;

    // Quantidade final ajustada depois do corte no Sankhya (QTDNEG ou QTDCONF pós-ajuste)
    private Double qtdAjustada;

    // Só pra log mesmo
    private Integer codUsuario;

    private Instant createdAt;
    private Instant updatedAt;
}
