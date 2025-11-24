// src/main/java/com/cemear/fila_conferencia_api/auth/AuthController.java
package com.cemear.fila_conferencia_api.auth;

import com.cemear.fila_conferencia_api.auth.dto.AuthResponse;
import com.cemear.fila_conferencia_api.auth.dto.LoginRequest;
import com.cemear.fila_conferencia_api.auth.dto.PushTokenRequest;
import com.cemear.fila_conferencia_api.auth.dto.RegisterRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/auth")
@RequiredArgsConstructor
public class AuthController {

    private final AuthService authService;
    private final JwtService jwtService;

    // ---------- REGISTER ----------
    @PostMapping("/register")
    public ResponseEntity<AuthResponse> register(@RequestBody RegisterRequest req) {
        AuthResponse resp = authService.registrar(req);
        return ResponseEntity.ok(resp);
    }

    // ---------- LOGIN ----------
    @PostMapping("/login")
    public ResponseEntity<AuthResponse> login(@RequestBody LoginRequest req) {
        AuthResponse resp = authService.login(req);
        return ResponseEntity.ok(resp);
    }

    // ---------- UPDATE PUSH TOKEN (FCM) ----------
    @PostMapping("/update-push-token")
    public ResponseEntity<Void> updatePushToken(
            @RequestBody PushTokenRequest req,
            @RequestHeader("Authorization") String authHeader
    ) {
        // "Bearer xxx" -> "xxx"
        String token = authHeader.replace("Bearer ", "");

        // subject do JWT = NOME do usuário (como você montou no AuthService)
        String nome = jwtService.extrairSubject(token);

        System.out.println("Update push token: usuario=" + nome + ", token=" + req.pushToken());

        authService.atualizarPushToken(nome, req.pushToken());
        return ResponseEntity.ok().build();
    }
}
