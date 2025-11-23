// src/main/java/com/cemear/fila_conferencia_api/auth/PushService.java
package com.cemear.fila_conferencia_api.auth.PushService;

import com.google.firebase.messaging.FirebaseMessaging;
import com.google.firebase.messaging.FirebaseMessagingException;
import com.google.firebase.messaging.Message;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class PushService {

    public void enviarPush(String token, String titulo, String corpo) {
        try {
            // 🔹 Notification = o que aparece visualmente no Android
            com.google.firebase.messaging.Notification notification =
                    com.google.firebase.messaging.Notification.builder()
                            .setTitle(titulo)
                            .setBody(corpo)
                            .build();

            // 🔹 Data = se você quiser ainda ler no app (onMessage)
            Message message = Message.builder()
                    .setToken(token)
                    .setNotification(notification)      // <- ESSENCIAL pro banner
                    .putData("title", titulo)
                    .putData("body", corpo)
                    .build();

            String response = FirebaseMessaging.getInstance().send(message);
            log.info("Push enviado com sucesso! ID: {}", response);

        } catch (FirebaseMessagingException e) {
            log.error("Erro ao enviar push: {}", e.getMessage(), e);
        }
    }
}
