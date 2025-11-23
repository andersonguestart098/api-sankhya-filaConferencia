// src/main/java/com/cemear/fila_conferencia_api/auth/Usuario.java
package com.cemear.fila_conferencia_api.auth;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.List;

@Data
@Document("usuarios")
public class Usuario {

    @Id
    private String id;

    private String nome;
    private String email;

    // senha sempre hash (BCrypt), nunca em texto puro
    private String senhaHash;

    private String avatarUrl;

    // já pensando no Firebase
    private List<String> deviceTokens;
}
