package com.cemear.fila_conferencia_api.sankhya;

import com.cemear.fila_conferencia_api.config.SankhyaProperties;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.RequiredArgsConstructor;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

@Service
@RequiredArgsConstructor
public class SankhyaAuthService {

    private final RestTemplate restTemplate;
    private final SankhyaProperties properties;

    private String cachedToken;
    private Instant tokenExpiration;

    public String getToken() {
        if (cachedToken != null && tokenExpiration != null &&
                tokenExpiration.isAfter(Instant.now().plusSeconds(30))) {
            return cachedToken;
        }
        return refreshToken();
    }

    public synchronized String refreshToken() {
        // se ainda está válido, apenas devolve
        if (cachedToken != null && tokenExpiration != null &&
                tokenExpiration.isAfter(Instant.now().plusSeconds(30))) {
            return cachedToken;
        }

        // ====== HEADERS ======
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
        headers.setAccept(List.of(MediaType.APPLICATION_JSON));
        headers.set("X-Token", properties.getXToken());

        // ====== BODY x-www-form-urlencoded ======
        MultiValueMap<String, String> body = new LinkedMultiValueMap<>();
        body.add("client_id", properties.getClientId());
        body.add("client_secret", properties.getClientSecret());
        body.add("grant_type", "client_credentials");

        HttpEntity<MultiValueMap<String, String>> request =
                new HttpEntity<>(body, headers);

        ResponseEntity<JsonNode> responseEntity = restTemplate.postForEntity(
                properties.getAuthUrl(),
                request,
                JsonNode.class
        );

        JsonNode response = responseEntity.getBody();
        if (response == null) {
            throw new IllegalStateException("Resposta vazia do /authenticate do Sankhya");
        }

        // normalmente vem "access_token" mesmo, como você já testou
        String token = response.path("access_token").asText(null);
        if (token == null || token.isBlank()) {
            throw new IllegalStateException(
                    "Não foi possível extrair o token do retorno do Sankhya: " + response
            );
        }

        this.cachedToken = token;

        // usa o expires_in que vem da API (em segundos)
        long expiresIn = response.path("expires_in").asLong(180L); // Sankhya ~3min
        // coloca uma margem de segurança de 30s
        long safeSeconds = Math.max(60, expiresIn - 30);

        this.tokenExpiration = Instant.now().plusSeconds(safeSeconds);

        return token;
    }

}
