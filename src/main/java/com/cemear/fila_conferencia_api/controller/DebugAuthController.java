package com.cemear.fila_conferencia_api.controller;

import com.cemear.fila_conferencia_api.sankhya.SankhyaAuthService;
import com.cemear.fila_conferencia_api.sankhya.SankhyaGatewayClient;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/debug")
@RequiredArgsConstructor
public class DebugAuthController {

    private final SankhyaAuthService authService;
    private final SankhyaGatewayClient gatewayClient;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @GetMapping("/token")
    public ResponseEntity<String> getToken() {
        String token = authService.getToken();
        return ResponseEntity.ok(token);
    }

    @PostMapping("/gateway")
    public ResponseEntity<JsonNode> callGateway(
            @RequestParam("serviceName") String serviceName,
            @RequestBody JsonNode body
    ) {
        JsonNode result = gatewayClient.callService(serviceName, body);
        return ResponseEntity.ok(result);
    }
}
