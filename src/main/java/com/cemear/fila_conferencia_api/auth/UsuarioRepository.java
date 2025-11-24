// src/main/java/com/cemear/fila_conferencia_api/auth/UsuarioRepository.java
package com.cemear.fila_conferencia_api.auth;

import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.Optional;

public interface UsuarioRepository extends MongoRepository<Usuario, String> {

    // 🔹 já existia e é usado no login
    Optional<Usuario> findByNome(String nome);

    // 🔹 já existia e é usado no register
    boolean existsByNome(String nome);

    // 🔥 novo: para casar com NOMEUSU do Sankhya sem se importar com maiúsculas/minúsculas
    Optional<Usuario> findByNomeIgnoreCase(String nome);

    // 🔥 opcional, mas bem útil: evitar "Manoel" e "manoel" duplicados
    boolean existsByNomeIgnoreCase(String nome);
}
