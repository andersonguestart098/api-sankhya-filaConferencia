// src/main/java/com/cemear/fila_conferencia_api/auth/dto/FinalizarDivergenteRequest.java
package com.cemear.fila_conferencia_api.auth.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

import java.util.List;

public record FinalizarDivergenteRequest(
        @NotNull Long nuconf,
        @NotNull Long nunotaOrig,
        @NotNull Long codUsuario,
        @Valid List<ItemFinalizacaoDTO> itens
) {
    public record ItemFinalizacaoDTO(
            @NotNull Integer sequencia,
            @NotNull Integer codProd,
            @NotNull Double qtdConferida  // 👈 MUDEI de qtdNeg para qtdConferida
    ) {}
}