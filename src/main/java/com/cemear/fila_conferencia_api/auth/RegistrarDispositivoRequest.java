// src/main/java/com/cemear/fila_conferencia_api/auth/RegistrarDispositivoRequest.java
package com.cemear.fila_conferencia_api.auth;

public record RegistrarDispositivoRequest(
        String usuarioId,
        String fcmToken
) {}
