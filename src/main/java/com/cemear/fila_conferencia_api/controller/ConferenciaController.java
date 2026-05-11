package com.cemear.fila_conferencia_api.controller;

import com.cemear.fila_conferencia_api.auth.dto.FinalizarDivergenteRequest;
import com.cemear.fila_conferencia_api.conferencia.ConferenciaCriadaDto;
import com.cemear.fila_conferencia_api.conferencia.ConferenciaWorkflowService;
import com.cemear.fila_conferencia_api.conferencia.FinalizarConferenciaRequest;
import com.cemear.fila_conferencia_api.conferencia.IniciarConferenciaRequest;
import com.cemear.fila_conferencia_api.conferencia.PedidoConferenciaDto;
import com.cemear.fila_conferencia_api.conferencia.PedidoConferenciaEventPublisher;
import com.cemear.fila_conferencia_api.conferencia.PreencherItensRequest;
import com.cemear.fila_conferencia_api.conferencia.cache.PedidoConferenciaCacheService;
import com.cemear.fila_conferencia_api.conferencia.mongo.ConferenciaItemMongoService;
import com.cemear.fila_conferencia_api.conferencia.mongo.PedidoConferenciaMongoService;
import com.cemear.fila_conferencia_api.conferencia.sse.SseHub;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/conferencia")
@RequiredArgsConstructor
@Slf4j
public class ConferenciaController {

    private final PedidoConferenciaCacheService pedidoConferenciaCacheService;
    private final ConferenciaWorkflowService conferenciaWorkflowService;
    private final ConferenciaItemMongoService conferenciaItemMongoService;
    private final PedidoConferenciaMongoService pedidoConferenciaMongoService;
    private final PedidoConferenciaEventPublisher eventPublisher;
    private final SseHub sseHub;

    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream() {
        log.info("📡 SSE client conectado");
        return sseHub.addClient();
    }

    @GetMapping("/pedidos-pendentes")
    public ResponseEntity<Map<String, Object>> listarPendentes(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "75") int pageSize,
            @RequestParam(required = false) String status,
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate dataIni,
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate dataFim,
            @RequestParam(required = false) Long nunota
    ) {
        log.info(
                "LISTAR_PENDENTES_CACHE page={}, pageSize={}, status={}, dataIni={}, dataFim={}, nunota={}",
                page, pageSize, status, dataIni, dataFim, nunota
        );

        List<PedidoConferenciaDto> pedidos = pedidoConferenciaCacheService.listarDoCache(
                page,
                pageSize,
                status,
                dataIni,
                dataFim,
                nunota
        );

        long total = pedidoConferenciaCacheService.contarDoCache(
                status,
                dataIni,
                dataFim,
                nunota
        );

        log.info("Retornando {} pedidos do cache / total={}", pedidos.size(), total);

        return ResponseEntity.ok(Map.of(
                "pedidos", pedidos,
                "total", total,
                "page", page,
                "pageSize", pageSize
        ));
    }

    @PostMapping("/iniciar")
    public ResponseEntity<?> iniciar(@RequestBody IniciarConferenciaRequest req) {
        log.info("🚀 INICIAR_CONFERENCIA - nunotaOrig: {}, codUsuario: {}",
                req.nunotaOrig(), req.codUsuario());

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

        log.info("✅ CONFERENCIA_CRIADA - nunotaOrig: {}, nuconf: {}",
                req.nunotaOrig(), nuconf);

        try {
            conferenciaWorkflowService.atualizarNuconfCabecalhoNota(
                    req.nunotaOrig(),
                    nuconf
            );

            log.info("✅ NUCONF_ATUALIZADO_TGFCAB - nunotaOrig: {}, nuconf: {}",
                    req.nunotaOrig(), nuconf);
        } catch (Exception e) {
            log.error("❌ ERRO_ATUALIZAR_NUCONF_TGFCAB - nunotaOrig: {}, nuconf: {}",
                    req.nunotaOrig(), nuconf, e);
        }

        try {
            pedidoConferenciaMongoService.atualizarStatusConferencia(
                    req.nunotaOrig(),
                    "AC",
                    nuconf
            );

            eventPublisher.pedidoStatusChanged(
                    req.nunotaOrig(),
                    "AC",
                    false
            );

            log.info("📡 EVENTO_CONFERENCIA_INICIADA_ENVIADO - nunotaOrig: {}, nuconf: {}",
                    req.nunotaOrig(), nuconf);
        } catch (Exception e) {
            log.warn("⚠️ FALHA_ATUALIZAR_MONGO_OU_EVENTO_INICIO - nunotaOrig: {}, nuconf: {}",
                    req.nunotaOrig(), nuconf, e);
        }

        try {
            conferenciaWorkflowService.preencherItensConferencia(
                    req.nunotaOrig(),
                    nuconf
            );

            log.info("✅ ITENS_PREENCHIDOS - nunotaOrig: {}, nuconf: {}",
                    req.nunotaOrig(), nuconf);
        } catch (Exception e) {
            log.warn("⚠️ FALHA_PREENCHER_ITENS - nunota={}, nuconf={}",
                    req.nunotaOrig(), nuconf, e);
        }

        ConferenciaCriadaDto dtoResposta = new ConferenciaCriadaDto(
                nuconf,
                req.nunotaOrig()
        );

        log.info("✅ CONFERENCIA_INICIADA - Response: {}", dtoResposta);

        return ResponseEntity.ok(dtoResposta);
    }

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

    @PostMapping("/conferente")
    public ResponseEntity<?> definirConferente(@RequestBody Map<String, Object> req) {
        Long nunota = Long.valueOf(req.get("nunota").toString());
        String nome = req.get("nome").toString();
        Integer codUsuario = Integer.valueOf(req.get("codUsuario").toString());

        log.info("👤 DEFINIR_CONFERENTE - nunota={}, codUsuario={}, nome={}",
                nunota, codUsuario, nome);

        conferenciaWorkflowService.definirConferente(
                nunota,
                codUsuario,
                nome
        );

        return ResponseEntity.ok().build();
    }

    @PostMapping("/finalizar")
    public ResponseEntity<?> finalizar(@RequestBody FinalizarConferenciaRequest req) {
        log.info("🏁 FINALIZAR_CONFERENCIA - nuconf: {}, nunotaOrig: {}, codUsuario: {}",
                req.nuconf(), req.nunotaOrig(), req.codUsuario());

        JsonNode resp = conferenciaWorkflowService.finalizarConferenciaOkComItens(
                req.nuconf(),
                req.codUsuario()
        );

        try {
            pedidoConferenciaMongoService.atualizarStatusConferencia(
                    req.nunotaOrig(),
                    "F",
                    req.nuconf()
            );

            eventPublisher.pedidoFinalizado(
                    req.nunotaOrig(),
                    req.nuconf()
            );

            log.info("📡 EVENTO_PEDIDO_FINALIZADO_ENVIADO - nunotaOrig: {}, nuconf: {}",
                    req.nunotaOrig(), req.nuconf());
        } catch (Exception e) {
            log.warn("⚠️ FALHA_ATUALIZAR_MONGO_OU_EVENTO_FINALIZACAO - nunotaOrig: {}, nuconf: {}",
                    req.nunotaOrig(), req.nuconf(), e);
        }

        log.info("✅ CONFERENCIA_FINALIZADA - nuconf: {}, nunotaOrig: {}",
                req.nuconf(), req.nunotaOrig());

        return ResponseEntity.ok(resp);
    }

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

    @GetMapping("/health")
    public ResponseEntity<?> health() {
        log.info("❤️ HEALTH_CHECK");

        return ResponseEntity.ok(Map.of(
                "status", "UP",
                "service", "Conferencia API",
                "timestamp", LocalDate.now().toString()
        ));
    }
}