// src/main/java/com/cemear/fila_conferencia_api/controller/ConferenciaController.java
package com.cemear.fila_conferencia_api.controller;

import com.cemear.fila_conferencia_api.auth.dto.FinalizarDivergenteRequest;
import com.cemear.fila_conferencia_api.conferencia.ConferenciaCriadaDto;
import com.cemear.fila_conferencia_api.conferencia.ConferenciaWorkflowService;
import com.cemear.fila_conferencia_api.conferencia.FinalizarConferenciaRequest;
import com.cemear.fila_conferencia_api.conferencia.IniciarConferenciaRequest;
import com.cemear.fila_conferencia_api.conferencia.PedidoConferenciaDto;
import com.cemear.fila_conferencia_api.conferencia.PedidoConferenciaService;
import com.cemear.fila_conferencia_api.conferencia.PreencherItensRequest;
import com.cemear.fila_conferencia_api.conferencia.mongo.ConferenciaItemMongoService;
import com.cemear.fila_conferencia_api.conferencia.mongo.ItemConferenciaDoc;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@RestController
@RequestMapping("/api/conferencia")
@RequiredArgsConstructor
@Slf4j
public class ConferenciaController {

    private final PedidoConferenciaService pedidoConferenciaService;
    private final ConferenciaWorkflowService conferenciaWorkflowService;
    private final ConferenciaItemMongoService conferenciaItemMongoService;

    // 1) Lista pedidos pendentes (paginado + filtros)
    @GetMapping("/pedidos-pendentes")
    public List<PedidoConferenciaDto> listarPendentes(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "50") int pageSize,
            @RequestParam(required = false) String status,
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate dataIni,
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate dataFim
    ) {
        return pedidoConferenciaService.listarPendentesPaginado(
                page,
                pageSize,
                status,
                dataIni,
                dataFim
        );
    }

    // 2) Inicia conferência para um pedido escolhido
    @PostMapping("/iniciar")
    public ResponseEntity<?> iniciar(@RequestBody IniciarConferenciaRequest req) {
        log.info("🚀 INICIAR_CONFERENCIA - nunotaOrig: {}, codUsuario: {}",
                req.nunotaOrig(), req.codUsuario());

        // 2.1 Cria a conferência no Sankhya
        JsonNode resp = conferenciaWorkflowService.iniciarConferencia(
                req.nunotaOrig(),
                req.codUsuario()
        );

        JsonNode nuconfNode = resp
                .path("responseBody")
                .path("entities")
                .path("entity")
                .path("NUCONF")
                .path("$");

        if (nuconfNode.isMissingNode() || nuconfNode.isNull()) {
            log.error("❌ NUCONF_NAO_ENCONTRADO - Resposta do Sankhya: {}", resp.toPrettyString());
            throw new IllegalStateException("Não consegui ler NUCONF da resposta do Sankhya: " + resp);
        }

        Long nuconf = nuconfNode.asLong();
        log.info("✅ CONFERENCIA_CRIADA - nunotaOrig: {}, nuconf: {}", req.nunotaOrig(), nuconf);

        // 2.2 Atualiza NUCONF no cabeçalho da nota (TGFCAB)
        try {
            conferenciaWorkflowService.atualizarNuconfCabecalhoNota(
                    req.nunotaOrig(),
                    nuconf
            );
            log.info("✅ NUCONF_ATUALIZADO_TGFCAB - nunotaOrig: {}, nuconf: {}", req.nunotaOrig(), nuconf);
        } catch (Exception e) {
            log.error("❌ ERRO_ATUALIZAR_NUCONF_TGFCAB - nunotaOrig: {}, nuconf: {}",
                    req.nunotaOrig(), nuconf, e);
        }

        // 2.3 Preenche itens da conferência (TGFCOI2)
        try {
            conferenciaWorkflowService.preencherItensConferencia(
                    req.nunotaOrig(),
                    nuconf
            );
            log.info("✅ ITENS_PREENCHIDOS - nunotaOrig: {}, nuconf: {}", req.nunotaOrig(), nuconf);
        } catch (Exception e) {
            // Se já existirem itens ou der algum problema pontual, loga e segue
            log.warn(
                    "⚠️  FALHA_PREENCER_ITENS - nunota={}, nuconf={}",
                    req.nunotaOrig(), nuconf, e
            );
        }

        // 2.4 Resposta pro front
        ConferenciaCriadaDto dtoResposta = new ConferenciaCriadaDto(
                nuconf,
                req.nunotaOrig()
        );

        log.info("✅ CONFERENCIA_INICIADA - Response: {}", dtoResposta);
        return ResponseEntity.ok(dtoResposta);
    }

    // Rota opcional para reprocessar/preencher itens manualmente, se necessário
    @PostMapping("/preencher-itens")
    public ResponseEntity<?> preencherItens(@RequestBody PreencherItensRequest req) {
        log.info("🔄 PREENCHER_ITENS_MANUAL - nunotaOrig: {}, nuconf: {}",
                req.nunotaOrig(), req.nuconf());

        JsonNode resp = conferenciaWorkflowService.preencherItensConferencia(
                req.nunotaOrig(),
                req.nuconf()
        );

        log.info("✅ ITENS_PREENCHIDOS_MANUAL - nunotaOrig: {}, nuconf: {}",
                req.nunotaOrig(), req.nuconf());
        return ResponseEntity.ok(resp);
    }

    // 3) Finaliza conferência (STATUS = 'F')
    @PostMapping("/finalizar")
    public ResponseEntity<?> finalizar(@RequestBody FinalizarConferenciaRequest req) {
        log.info("🏁 FINALIZAR_CONFERENCIA - nuconf: {}, codUsuario: {}",
                req.nuconf(), req.codUsuario());

        conferenciaWorkflowService.finalizarConferencia(
                req.nuconf(),
                req.codUsuario()
        );

        log.info("✅ CONFERENCIA_FINALIZADA - nuconf: {}", req.nuconf());
        return ResponseEntity.ok().build();
    }

    // 4) Finaliza conferência divergente
    @PostMapping("/finalizar-divergente")
    public ResponseEntity<?> finalizarDivergente(@RequestBody FinalizarDivergenteRequest req) {
        log.info("🎯 FINALIZAR_DIVERGENTE - nuconf: {}, nunotaOrig: {}, codUsuario: {}, itens: {}",
                req.nuconf(), req.nunotaOrig(), req.codUsuario(),
                req.itens() != null ? req.itens().size() : 0);

        JsonNode result = conferenciaWorkflowService.finalizarConferenciaDivergente(req);

        log.info("✅ CONFERENCIA_DIVERGENTE_FINALIZADA - nuconf: {}, nunotaOrig: {}",
                req.nuconf(), req.nunotaOrig());

        return ResponseEntity.ok(result);
    }
    

    // 6) Health check simples
    @GetMapping("/health")
    public ResponseEntity<?> health() {
        log.info("❤️  HEALTH_CHECK");
        return ResponseEntity.ok(Map.of(
                "status", "UP",
                "service", "Conferencia API",
                "timestamp", LocalDate.now().toString()
        ));
    }
}