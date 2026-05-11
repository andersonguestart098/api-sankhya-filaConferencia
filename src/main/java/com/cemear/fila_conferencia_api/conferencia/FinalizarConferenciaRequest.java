package com.cemear.fila_conferencia_api.conferencia;

public record FinalizarConferenciaRequest(
        Long nuconf,
        Long nunotaOrig,
        Long codUsuario
) {}