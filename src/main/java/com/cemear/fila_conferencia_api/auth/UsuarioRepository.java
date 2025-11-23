// src/main/java/com/cemear/fila_conferencia_api/auth/UsuarioRepository.java
package com.cemear.fila_conferencia_api.auth;

import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.Optional;

public interface UsuarioRepository extends MongoRepository<Usuario, String> {
    Optional<Usuario> findByNome(String nome);
    boolean existsByEmail(String email); // pode deixar se quiser continuar validando no register
}
