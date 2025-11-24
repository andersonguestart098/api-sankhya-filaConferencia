// src/main/java/com/cemear/fila_conferencia_api/auth/JwtService.java
package com.cemear.fila_conferencia_api.auth;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.security.Keys;
import org.springframework.stereotype.Service;

import java.security.Key;
import java.util.Date;
import java.util.function.Function;

@Service
public class JwtService {

    // em produção, isso deveria vir de configuração (application.yml)
    // aqui está fixo só pra simplificar o exemplo
    private final Key key = Keys.secretKeyFor(SignatureAlgorithm.HS256);

    // 8h de expiração só pra exemplo
    private final long EXPIRATION_MS = 8 * 60 * 60 * 1000;

    /**
     * Gera um JWT onde:
     *  - subject = NOME do usuário (usado depois para atualizar pushToken)
     *  - claim "userId" = ID do usuário no banco
     */
    public String gerarToken(String usuarioId, String nome) {
        Date agora = new Date();
        Date expira = new Date(agora.getTime() + EXPIRATION_MS);

        return Jwts.builder()
                .setSubject(nome)                 // <- SUBJECT = NOME
                .setIssuer("fila-conferencia-api")
                .claim("userId", usuarioId)       // <- ID vai em uma claim separada
                .setIssuedAt(agora)
                .setExpiration(expira)
                .signWith(key)
                .compact();
    }

    /**
     * Extrai o subject (nome do usuário) do token.
     */
    public String extrairSubject(String token) {
        return extrairClaim(token, Claims::getSubject);
    }

    /**
     * Se no futuro você quiser ler o userId:
     */
    public String extrairUserId(String token) {
        return extrairClaim(token, claims -> claims.get("userId", String.class));
    }

    // ----------------- utilitários internos -----------------

    public <T> T extrairClaim(String token, Function<Claims, T> claimsResolver) {
        final Claims claims = extrairTodosClaims(token);
        return claimsResolver.apply(claims);
    }

    private Claims extrairTodosClaims(String token) {
        return Jwts.parserBuilder()
                .setSigningKey(key)
                .build()
                .parseClaimsJws(token)
                .getBody();
    }
}
