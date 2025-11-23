package com.cemear.fila_conferencia_api.auth;

import com.google.firebase.messaging.FirebaseMessaging;
import com.google.firebase.messaging.Message;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class PushService {

    public void enviarPush(String pushToken, String titulo, String corpo) {
        try {
            Message message = Message.builder()
                    .setToken(pushToken)
                    .putData("title", titulo)
                    .putData("body", corpo)
                    .build();

            String response = FirebaseMessaging.getInstance().send(message);
            log.info("Push enviado com sucesso! ID: {}", response);

        } catch (Exception e) {
            log.error("Erro ao enviar push: {}", e.getMessage(), e);
        }
    }
}
