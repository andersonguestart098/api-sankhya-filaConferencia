package com.cemear.fila_conferencia_api.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Getter
@Setter
@Configuration
@ConfigurationProperties(prefix = "sankhya")
public class SankhyaProperties {

    private String baseUrl;      // se for usar depois pro gateway
    private String authUrl;

    private String xToken;       // header X-Token
    private String clientId;     // form client_id
    private String clientSecret; // form client_secret
}
