// src/main/java/com/cemear/fila_conferencia_api/auth/dto/RegisterRequest.java
package com.cemear.fila_conferencia_api.auth.dto;

public record RegisterRequest(
        String nome,
        String email,
        String senha,
        String avatarUrl
) {}
