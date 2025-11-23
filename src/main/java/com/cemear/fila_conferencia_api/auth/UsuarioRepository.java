// src/main/java/com/cemear/fila_conferencia_api/auth/UsuarioRepository.java
package com.cemear.fila_conferencia_api.auth;

import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.Optional;

public interface UsuarioRepository extends MongoRepository<Usuario, String> {

    Optional<Usuario> findByNome(String nome);

    // 👇 adiciona isso:
    boolean existsByNome(String nome);
}
