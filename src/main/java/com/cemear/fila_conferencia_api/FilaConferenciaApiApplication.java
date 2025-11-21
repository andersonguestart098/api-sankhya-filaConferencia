package com.cemear.fila_conferencia_api;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class FilaConferenciaApiApplication {

    public static void main(String[] args) {
        SpringApplication.run(FilaConferenciaApiApplication.class, args);
    }
}
