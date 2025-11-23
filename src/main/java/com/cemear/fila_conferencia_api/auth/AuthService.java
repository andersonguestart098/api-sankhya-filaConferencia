// src/main/java/com/cemear/fila_conferencia_api/auth/AuthService.java
package com.cemear.fila_conferencia_api.auth;

import com.cemear.fila_conferencia_api.auth.dto.AuthResponse;
import com.cemear.fila_conferencia_api.auth.dto.LoginRequest;
import com.cemear.fila_conferencia_api.auth.dto.RegisterRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class AuthService {

    private final UsuarioRepository usuarioRepository;
    private final JwtService jwtService;

    private final BCryptPasswordEncoder passwordEncoder = new BCryptPasswordEncoder();

    public AuthResponse registrar(RegisterRequest req) {
        if (usuarioRepository.existsByEmail(req.email())) {
            throw new IllegalArgumentException("E-mail já cadastrado.");
        }

        Usuario u = new Usuario();
        u.setNome(req.nome());
        u.setEmail(req.email());
        u.setSenhaHash(passwordEncoder.encode(req.senha()));
        u.setAvatarUrl(req.avatarUrl());

        usuarioRepository.save(u);

        String token = jwtService.gerarToken(u.getId(), u.getEmail());

        return new AuthResponse(
                token,
                u.getId(),
                u.getNome(),
                u.getEmail(),
                u.getAvatarUrl()
        );
    }

    public AuthResponse login(LoginRequest req) {
        Usuario u = usuarioRepository.findByNome(req.nome())
                .orElseThrow(() -> new IllegalArgumentException("Usuário ou senha inválidos."));

        if (!passwordEncoder.matches(req.senha(), u.getSenhaHash())) {
            throw new IllegalArgumentException("Usuário ou senha inválidos.");
        }

        String token = jwtService.gerarToken(u.getId(), u.getEmail());

        return new AuthResponse(
                token,
                u.getId(),
                u.getNome(),
                u.getEmail(),
                u.getAvatarUrl()
        );
    }

}
