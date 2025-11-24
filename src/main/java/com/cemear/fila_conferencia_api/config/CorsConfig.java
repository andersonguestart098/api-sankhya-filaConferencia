package com.cemear.fila_conferencia_api.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class CorsConfig implements WebMvcConfigurer {

    @Override
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/**")
                // libera as origens do teu frontend web
                .allowedOrigins(
                        "http://localhost:3000",   // React local
                        "http://localhost:5173",    // Vite (se usar)
                        "https://fila-conferencia-dash-irbh.vercel.app"
                        // adiciona aqui o domínio em produção depois
                        // "https://fila-conferencia-web.vercel.app"
                )
                .allowedMethods("GET", "POST", "PUT", "DELETE", "OPTIONS")
                .allowedHeaders("*")
                .allowCredentials(true);
    }
}
