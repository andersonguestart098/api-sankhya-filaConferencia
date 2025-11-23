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

    // ---------- REGISTER ----------
    public AuthResponse registrar(RegisterRequest req) {
        // valida nome único (em vez de e-mail)
        if (usuarioRepository.existsByNome(req.nome())) {
            throw new IllegalArgumentException("Nome de usuário já cadastrado.");
        }

        Usuario u = new Usuario();
        u.setNome(req.nome());
        // email agora é opcional, se quiser manter no DTO:
        u.setEmail(req.email());
        u.setSenhaHash(passwordEncoder.encode(req.senha()));
        u.setAvatarUrl(req.avatarUrl());

        usuarioRepository.save(u);

        // 👇 subject do token agora é o NOME
        String token = jwtService.gerarToken(u.getId(), u.getNome());

        return new AuthResponse(
                token,
                u.getId(),
                u.getNome(),
                u.getEmail(),
                u.getAvatarUrl()
        );
    }

    // ---------- LOGIN (por nome + senha) ----------
    public AuthResponse login(LoginRequest req) {
        Usuario u = usuarioRepository.findByNome(req.nome())
                .orElseThrow(() -> new IllegalArgumentException("Usuário ou senha inválidos."));

        if (!passwordEncoder.matches(req.senha(), u.getSenhaHash())) {
            throw new IllegalArgumentException("Usuário ou senha inválidos.");
        }

        // idem: subject = nome
        String token = jwtService.gerarToken(u.getId(), u.getNome());

        return new AuthResponse(
                token,
                u.getId(),
                u.getNome(),
                u.getEmail(),
                u.getAvatarUrl()
        );
    }

    // ---------- ATUALIZAR PUSH TOKEN ----------
    public void atualizarPushToken(String nome, String pushToken) {
        Usuario usuario = usuarioRepository.findByNome(nome)
                .orElseThrow(() -> new RuntimeException("Usuário não encontrado para nome: " + nome));

        usuario.setPushToken(pushToken);
        usuarioRepository.save(usuario);

        System.out.println("Push token salvo para " + nome + ": " + pushToken);
    }

}
