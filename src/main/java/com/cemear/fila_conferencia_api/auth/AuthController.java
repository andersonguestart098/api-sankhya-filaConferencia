// src/main/java/com/cemear/fila_conferencia_api/auth/AuthController.java
package com.cemear.fila_conferencia_api.auth;

import com.cemear.fila_conferencia_api.auth.dto.AuthResponse;
import com.cemear.fila_conferencia_api.auth.dto.LoginRequest;
import com.cemear.fila_conferencia_api.auth.dto.RegisterRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/auth")
@RequiredArgsConstructor
public class AuthController {

    private final AuthService authService;

    @PostMapping("/register")
    public ResponseEntity<AuthResponse> register(@RequestBody RegisterRequest req) {
        AuthResponse resp = authService.registrar(req);
        return ResponseEntity.ok(resp);
    }

    @PostMapping("/login")
    public ResponseEntity<AuthResponse> login(@RequestBody LoginRequest req) {
        AuthResponse resp = authService.login(req);
        return ResponseEntity.ok(resp);
    }
}
