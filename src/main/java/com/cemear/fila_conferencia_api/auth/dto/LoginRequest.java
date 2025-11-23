// src/main/java/com/cemear/fila_conferencia_api/auth/dto/LoginRequest.java
package com.cemear.fila_conferencia_api.auth.dto;

public record LoginRequest(
        String nome,
        String senha
) {}
