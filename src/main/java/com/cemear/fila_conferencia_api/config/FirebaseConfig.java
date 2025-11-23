package com.cemear.fila_conferencia_api.config;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

@Configuration
@Slf4j
public class FirebaseConfig {

    @PostConstruct
    public void init() {
        try {
            String firebaseJson = System.getenv("FIREBASE_SERVICE_ACCOUNT");

            if (firebaseJson == null || firebaseJson.isBlank()) {
                log.warn("FIREBASE_SERVICE_ACCOUNT não configurado no Heroku! Push não vai funcionar.");
                return;
            }

            FirebaseOptions options = FirebaseOptions.builder()
                    .setCredentials(
                            GoogleCredentials.fromStream(
                                    new ByteArrayInputStream(firebaseJson.getBytes(StandardCharsets.UTF_8))
                            )
                    )
                    .build();

            if (FirebaseApp.getApps().isEmpty()) {
                FirebaseApp.initializeApp(options);
                log.info("FirebaseApp inicializado com sucesso!");
            }

        } catch (Exception e) {
            log.error("Erro ao inicializar FirebaseApp", e);
        }
    }
}
