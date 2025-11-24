// src/main/java/com/cemear/fila_conferencia_api/auth/UsuarioRepository.java
package com.cemear.fila_conferencia_api.auth;

import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.Optional;

public interface UsuarioRepository extends MongoRepository<Usuario, String> {

    // usado no login (como você já tinha)
    Optional<Usuario> findByNome(String nome);

    // 🔥 novo: usado na conferência para casar com NOMEUSU do Sankhya
    Optional<Usuario> findByNomeIgnoreCase(String nome);
}
