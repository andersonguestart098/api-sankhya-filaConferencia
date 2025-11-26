// src/main/java/com/cemear/fila_conferencia_api/conferencia/FinalizarDivergenteRequest.java
package com.cemear.fila_conferencia_api.auth.dto;

import java.util.List;

public record FinalizarDivergenteRequest(
        Long nuconf,
        Long nunotaOrig,
        Long codUsuario,
        List<ItemFinalizacaoDTO> itens
) {
    public record ItemFinalizacaoDTO(
            Integer sequencia,
            Long codProd,
            Double qtdNeg,
            Double qtdConferida
    ) {}
}
