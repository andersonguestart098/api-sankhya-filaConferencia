// src/main/java/com/cemear/fila_conferencia_api/auth/PushService.java
package com.cemear.fila_conferencia_api.auth;

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
            // Notificação visual que aparece no Android
            com.google.firebase.messaging.Notification notification =
                    com.google.firebase.messaging.Notification.builder()
                            .setTitle(titulo)
                            .setBody(corpo)
                            .build();

            // Mensagem completa enviada ao FCM
            Message message = Message.builder()
                    .setToken(token)
                    .setNotification(notification) // importante pro banner
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
