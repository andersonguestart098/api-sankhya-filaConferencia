package com.cemear.fila_conferencia_api.controller;

import com.cemear.fila_conferencia_api.conferencia.*;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/conferencia")
@RequiredArgsConstructor
public class ConferenciaController {

    private final PedidoConferenciaService pedidoConferenciaService;
    private final ConferenciaWorkflowService conferenciaWorkflowService;

    // 1) Lista pedidos pendentes
    @GetMapping("/pedidos-pendentes")
    public ResponseEntity<?> listarPendentes() {
        return ResponseEntity.ok(pedidoConferenciaService.listarPendentes());
    }

    // 2) Inicia conferência para um pedido escolhido
    @PostMapping("/iniciar")
    public ResponseEntity<?> iniciar(@RequestBody IniciarConferenciaRequest req) {

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
            throw new IllegalStateException("Não consegui ler NUCONF da resposta do Sankhya: " + resp);
        }

        Long nuconf = nuconfNode.asLong();

        // Dispara CrudListener para vincular NUCONF ao cabeçalho da nota
        conferenciaWorkflowService.atualizarNuconfCabecalhoNota(
                req.nunotaOrig(),
                nuconf
        );

        ConferenciaCriadaDto dtoResposta = new ConferenciaCriadaDto(
                nuconf,
                req.nunotaOrig()
        );

        return ResponseEntity.ok(dtoResposta);
    }

    @PostMapping("/preencher-itens")
    public ResponseEntity<?> preencherItens(@RequestBody PreencherItensRequest req) {

        JsonNode resp = conferenciaWorkflowService.preencherItensConferencia(
                req.nunotaOrig(),
                req.nuconf()
        );

        // se quiser pode só devolver o JSON bruto do Sankhya:
        return ResponseEntity.ok(resp);
    }


    // 3) Finaliza conferência (STATUS = 'F')
    @PostMapping("/finalizar")
    public ResponseEntity<?> finalizar(@RequestBody FinalizarConferenciaRequest req) {

        conferenciaWorkflowService.finalizarConferencia(
                req.nuconf(),
                req.codUsuario()
        );

        // pode devolver só 204, mas pra front é até mais simples:
        return ResponseEntity.ok().build();
    }


}
