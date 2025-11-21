package com.cemear.fila_conferencia_api.sankhya;

import com.cemear.fila_conferencia_api.config.SankhyaProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.List;
import java.util.Objects;

@Slf4j
@Component
@RequiredArgsConstructor
public class SankhyaGatewayClient {

    private final RestTemplate restTemplate;
    private final SankhyaAuthService authService;
    private final SankhyaProperties properties;
    private final ObjectMapper objectMapper; // Spring injeta o ObjectMapper padrão

    // ------------ DbExplorer ------------
    public JsonNode executeDbExplorer(String sql) {
        String url = UriComponentsBuilder
                .fromHttpUrl(properties.getBaseUrl())
                .queryParam("serviceName", "DbExplorerSP.executeQuery")
                .queryParam("outputType", "json")
                .toUriString();

        JsonNode body = buildDbExplorerBody(sql);

        return exchangeWithRetry(url, body);
    }

    /**
     * Monta o body padrão esperado pelo DbExplorer:
     *
     * POST /mge/service.sbr?serviceName=DbExplorerSP.executeQuery&outputType=json
     * {
     *   "requestBody": {
     *     "sql": "SELECT ...",
     *     "parameters": {}
     *   }
     * }
     */
    private JsonNode buildDbExplorerBody(String sql) {
        ObjectNode root = objectMapper.createObjectNode();
        ObjectNode inner = objectMapper.createObjectNode();

        inner.put("sql", sql);
        // se quiser, pode parametrizar depois; por enquanto manda vazio
        inner.set("parameters", objectMapper.createObjectNode());

        root.set("requestBody", inner);
        return root;
    }

    // ------------ Genérico: CRUDService, etc. ------------
    public JsonNode callService(String serviceName, JsonNode requestBody) {
        String url = UriComponentsBuilder
                .fromHttpUrl(properties.getBaseUrl())
                .queryParam("serviceName", serviceName)
                .queryParam("outputType", "json")
                .toUriString();

        return exchangeWithRetry(url, requestBody);
    }

    // ------------ núcleo de retry 401/403 ------------
    private JsonNode exchangeWithRetry(String url, JsonNode body) {
        HttpHeaders headers = buildAuthHeaders(authService.getToken());
        HttpEntity<JsonNode> entity = new HttpEntity<>(body, headers);

        try {
            ResponseEntity<JsonNode> resp = restTemplate.exchange(
                    url,
                    HttpMethod.POST,
                    entity,
                    JsonNode.class
            );
            return ensureOk(resp);
        } catch (HttpClientErrorException.Forbidden | HttpClientErrorException.Unauthorized ex) {
            log.warn("Token possivelmente expirado ({}). Renovando e tentando novamente...",
                    ex.getStatusCode().value());

            // força renovar token
            String newToken = authService.refreshToken();
            HttpHeaders retryHeaders = buildAuthHeaders(newToken);
            HttpEntity<JsonNode> retryEntity = new HttpEntity<>(body, retryHeaders);

            ResponseEntity<JsonNode> retryResp = restTemplate.exchange(
                    url,
                    HttpMethod.POST,
                    retryEntity,
                    JsonNode.class
            );
            return ensureOk(retryResp);
        }
    }

    private HttpHeaders buildAuthHeaders(String token) {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        h.setAccept(List.of(MediaType.APPLICATION_JSON));
        h.setBearerAuth(token);
        return h;
    }

    private JsonNode ensureOk(ResponseEntity<JsonNode> resp) {
        JsonNode root = Objects.requireNonNull(resp.getBody(), "Resposta vazia da API Sankhya");
        String status = root.path("status").asText("");
        if (!"1".equals(status) && !status.isEmpty()) {
            String msg = root.path("statusMessage").asText("Falha desconhecida");
            throw new IllegalStateException("Sankhya retornou status != 1: " + msg);
        }
        return root;
    }
}
