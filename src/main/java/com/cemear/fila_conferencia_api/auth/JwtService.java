// src/main/java/com/cemear/fila_conferencia_api/auth/JwtService.java
package com.cemear.fila_conferencia_api.auth;

import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.security.Keys;
import org.springframework.stereotype.Service;

import java.security.Key;
import java.util.Date;

@Service
public class JwtService {

    // em produção, isso vem de configuração (application.yml)
    private final Key key = Keys.secretKeyFor(SignatureAlgorithm.HS256);

    // 8h de expiração só pra exemplo
    private final long EXPIRATION_MS = 8 * 60 * 60 * 1000;

    public String gerarToken(String usuarioId, String email) {
        Date agora = new Date();
        Date expira = new Date(agora.getTime() + EXPIRATION_MS);

        return Jwts.builder()
                .setSubject(usuarioId)
                .setIssuer("fila-conferencia-api")
                .claim("email", email)
                .setIssuedAt(agora)
                .setExpiration(expira)
                .signWith(key)
                .compact();
    }

    // depois dá pra adicionar métodos pra validar/ler token
}
