// src/main/java/com/cemear/fila_conferencia_api/auth/dto/AuthResponse.java
package com.cemear.fila_conferencia_api.auth.dto;

public record AuthResponse(
        String token,
        String usuarioId,
        String nome,
        String email,
        String avatarUrl
) {}
