package com.cemear.fila_conferencia_api.conferencia;

public record PedidoStatusRapidoDto(
        Long nunota,
        Long nuconf,
        String statusConferencia
) {}