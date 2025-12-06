// src/main/java/com/cemear/fila_conferencia_api/conferencia/ConferenciaWorkflowService.java
package com.cemear.fila_conferencia_api.conferencia;

import com.cemear.fila_conferencia_api.auth.dto.FinalizarDivergenteRequest;
import com.cemear.fila_conferencia_api.auth.dto.FinalizarDivergenteRequest.ItemFinalizacaoDTO;
import com.cemear.fila_conferencia_api.conferencia.mongo.ConferenciaItemMongoService;
import com.cemear.fila_conferencia_api.conferencia.mongo.ItemConferenciaDoc;
import com.cemear.fila_conferencia_api.sankhya.SankhyaGatewayClient;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

@Slf4j
@Service
@RequiredArgsConstructor
public class ConferenciaWorkflowService {

    private final SankhyaGatewayClient gatewayClient;
    private final ObjectMapper objectMapper;
    private final ConferenciaItemMongoService conferenciaItemMongoService;

    private static final DateTimeFormatter FMT =
            DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm");

    // =========================================================================
    // 1) INICIAR CONFERÊNCIA (CabecalhoConferencia / TGFCOI)
    // =========================================================================
    public JsonNode iniciarConferencia(Long nunotaOrig, Long codUsuario) {
        String agora = LocalDateTime.now().format(FMT);

        // Monta payload do CRUDServiceProvider.saveRecord
        ObjectNode root = objectMapper.createObjectNode();
        ObjectNode requestBody = root.putObject("requestBody");
        ObjectNode dataSet = requestBody.putObject("dataSet");

        dataSet.put("rootEntity", "CabecalhoConferencia");
        dataSet.put("includePresentationFields", "S");

        ObjectNode dataRow = dataSet.putObject("dataRow");
        ObjectNode localFields = dataRow.putObject("localFields");

        // STATUS A = Em Conferência
        localFields.putObject("STATUS").put("$", "A");
        localFields.putObject("DHINICONF").put("$", agora);
        localFields.putObject("DHFINCONF").put("$", agora);
        localFields.putObject("CODUSUCONF").put("$", codUsuario.toString());
        localFields.putObject("NUNOTAORIG").put("$", nunotaOrig.toString());

        ObjectNode entity = dataSet.putObject("entity");
        ObjectNode fieldSet = entity.putObject("fieldSet");
        fieldSet.put("list",
                "NUCONF,NUNOTAORIG,STATUS,DHINICONF,DHFINCONF,CODUSUCONF"
        );

        return gatewayClient.callService(
                "CRUDServiceProvider.saveRecord",
                root
        );
    }

    // =========================================================================
    // 2) ATUALIZAR NUCONF NO CABEÇALHO DA NOTA (TGFCAB)
    // =========================================================================
    public void atualizarNuconfCabecalhoNota(Long nunotaOrig, Long nuconf) {
        ObjectNode root = objectMapper.createObjectNode();
        ObjectNode requestBody = root.putObject("requestBody");
        ObjectNode dataSet = requestBody.putObject("dataSet");

        dataSet.put("rootEntity", "CabecalhoNota");
        dataSet.put("includePresentationFields", "S");

        ObjectNode dataRow = dataSet.putObject("dataRow");
        ObjectNode localFields = dataRow.putObject("localFields");
        localFields.putObject("NUCONFATUAL").put("$", nuconf.toString());

        ObjectNode key = dataRow.putObject("key");
        key.putObject("NUNOTA").put("$", nunotaOrig.toString());

        ObjectNode entity = dataSet.putObject("entity");
        ObjectNode fieldSet = entity.putObject("fieldset");
        fieldSet.put("list", "NUNOTA,NUCONFATUAL");

        gatewayClient.callService("CRUDServiceProvider.saveRecord", root);
    }

    // =========================================================================
    // 3) COPIAR ITENS DA TGFITE PARA TGFCOI2
    //    + REGISTRAR QTDNEG ORIGINAL NO MONGO
    // =========================================================================
    public JsonNode preencherItensConferencia(Long nunotaOrig, Long nuconf) {
        log.info("########## DEBUG_PREENCHE_ITENS nunotaOrig={} nuconf={} ##########",
                nunotaOrig, nuconf);

        String sql = """
            SELECT
                i.SEQUENCIA,
                i.CODPROD,
                p.DESCRPROD,
                i.CODVOL,
                i.CONTROLE,
                i.QTDNEG
            FROM TGFITE i
            JOIN TGFPRO p ON p.CODPROD = i.CODPROD
            WHERE i.NUNOTA = %d
            """.formatted(nunotaOrig);

        log.info("DEBUG_SQL_TGFITE nunotaOrig={} SQL:\n{}", nunotaOrig, sql);

        JsonNode root = gatewayClient.executeDbExplorer(sql);
        JsonNode responseBody = root.path("responseBody");
        JsonNode fieldsMetadata = responseBody.path("fieldsMetadata");
        JsonNode rowsNode = responseBody.path("rows");

        if (!rowsNode.isArray()) {
            log.error("Resposta inesperada do DbExplorer ao preencher itens conferência: {}",
                    root.toPrettyString());
            throw new IllegalStateException("DbExplorer não retornou array 'rows' na resposta.");
        }

        ArrayNode rows = (ArrayNode) rowsNode;
        List<String> cols = extractColumns(fieldsMetadata);

        int iSequencia = indexOf(cols, "SEQUENCIA");
        int iCodprod   = indexOf(cols, "CODPROD");
        int iCodvol    = indexOf(cols, "CODVOL");
        int iControle  = indexOf(cols, "CONTROLE");
        int iQtdneg    = indexOf(cols, "QTDNEG");

        // Monta payload para inserir em DetalhesConferencia (TGFCOI2)
        ObjectNode rootReq = objectMapper.createObjectNode();
        ObjectNode requestBody2 = rootReq.putObject("requestBody");
        ObjectNode dataSet = requestBody2.putObject("dataSet");

        dataSet.put("rootEntity", "DetalhesConferencia");
        dataSet.put("includePresentationFields", "S");

        ArrayNode dataRowArray = dataSet.putArray("dataRow");

        int count = 0;
        for (JsonNode r : rows) {
            if (!r.isArray()) continue;

            String seqConf = readText(r, iSequencia);
            Integer seqInt;

            // Garante SEQCONF sequencial caso venha nulo no TGFITE
            if (seqConf == null || seqConf.isBlank()) {
                seqInt = count + 1;
                seqConf = String.valueOf(seqInt);
            } else {
                try {
                    seqInt = Integer.valueOf(seqConf);
                } catch (NumberFormatException e) {
                    seqInt = count + 1;
                    seqConf = String.valueOf(seqInt);
                }
            }

            String codprodStr = readText(r, iCodprod);
            String codvol   = readText(r, iCodvol);
            String controle = readText(r, iControle);
            String qtdneg   = readText(r, iQtdneg);

            Integer codProdInt = null;
            if (codprodStr != null && !codprodStr.isBlank()) {
                try {
                    codProdInt = Integer.valueOf(codprodStr);
                } catch (NumberFormatException e) {
                    log.warn("Não consegui converter CODPROD para Integer. nunota={} seq={} codprod='{}'",
                            nunotaOrig, seqInt, codprodStr);
                }
            }

            Double qtdOriginal = null;
            if (qtdneg != null && !qtdneg.isBlank()) {
                try {
                    qtdOriginal = Double.valueOf(qtdneg);
                } catch (NumberFormatException e) {
                    log.warn(
                            "Não consegui converter QTDNEG para Double. nunota={}, seq={}, qtdneg='{}'",
                            nunotaOrig, seqInt, qtdneg
                    );
                }
            }

            // Snapshot da QTD original no Mongo pra histórico
            if (qtdOriginal != null) {
                try {
                    conferenciaItemMongoService.registrarQtdOriginal(
                            nunotaOrig,
                            seqInt,
                            codProdInt,
                            qtdOriginal,
                            null
                    );
                } catch (Exception e) {
                    log.warn("Falha ao registrar qtdOriginal no Mongo. nunota={} seq={}",
                            nunotaOrig, seqInt, e);
                }
            }

            // Monta linha para DetalhesConferencia
            ObjectNode rowNode = dataRowArray.addObject();
            ObjectNode local = rowNode.putObject("localFields");

            local.putObject("NUCONF").put("$", nuconf.toString());
            local.putObject("SEQCONF").put("$", seqConf);
            local.putObject("CODBARRA").put("$", "");
            local.putObject("CODPROD").put("$", codprodStr);
            local.putObject("CODVOL").put("$", codvol);
            local.putObject("CONTROLE").put("$", controle == null ? "" : controle);
            local.putObject("QTDCONFVOLPAD").put("$", qtdneg);
            local.putObject("QTDCONF").put("$", qtdneg);
            local.putObject("COPIA").put("$", "N");

            count++;
        }

        ObjectNode entity = dataSet.putObject("entity");
        ObjectNode fieldSet = entity.putObject("fieldSet");
        fieldSet.put("list",
                "NUCONF,SEQCONF,CODBARRA,CODPROD,CODVOL,CONTROLE,QTDCONFVOLPAD,QTDCONF,COPIA"
        );

        log.info("DEBUG_SAVE_DETALHES_REQUEST nunota={} payload:\n{}",
                nunotaOrig, rootReq.toPrettyString());

        return gatewayClient.callService(
                "CRUDServiceProvider.saveRecord",
                rootReq
        );
    }

    // =========================================================================
    // 4) FINALIZAR CONFERÊNCIA NORMAL (sem corte / STATUS = F)
    // =========================================================================
    public JsonNode finalizarConferencia(Long nuconf, Long codUsuario) {
        String agora = LocalDateTime.now().format(FMT);

        ObjectNode root = objectMapper.createObjectNode();
        ObjectNode requestBody = root.putObject("requestBody");
        ObjectNode dataSet = requestBody.putObject("dataSet");

        dataSet.put("rootEntity", "CabecalhoConferencia");
        dataSet.put("includePresentationFields", "S");

        ObjectNode dataRow = dataSet.putObject("dataRow");
        ObjectNode localFields = dataRow.putObject("localFields");

        localFields.putObject("STATUS").put("$", "F");
        localFields.putObject("DHFINCONF").put("$", agora);
        localFields.putObject("CODUSUCONF").put("$", codUsuario.toString());

        ObjectNode key = dataRow.putObject("key");
        key.putObject("NUCONF").put("$", nuconf.toString());

        ObjectNode entity = dataSet.putObject("entity");
        ObjectNode fieldSet = entity.putObject("fieldSet");
        fieldSet.put("list",
                "NUCONF,NUNOTAORIG,STATUS,DHINICONF,DHFINCONF,CODUSUCONF"
        );

        return gatewayClient.callService(
                "CRUDServiceProvider.saveRecord",
                root
        );
    }

    // =========================================================================
    // 5) FINALIZAR CONFERÊNCIA DIVERGENTE (com corte)
    //    - Atualiza TGFCOI2, TGFITE, TGFCAB e Mongo
    // =========================================================================
    public JsonNode finalizarConferenciaDivergente(FinalizarDivergenteRequest req) {
        Long nuconf = req.nuconf();
        Long nunotaOrig = req.nunotaOrig();
        Long codUsuario = req.codUsuario();
        List<ItemFinalizacaoDTO> itens = req.itens();

        // LOG COMPLETO DA REQ
        try {
            log.info(
                    "\n################## DEBUG_FINALIZAR_DIVERGENTE_REQ ##################\n" +
                            "nuconf={} nunotaOrig={} codUsuario={}\n" +
                            "Total de itens: {}\n" +
                            "PAYLOAD_COMPLETO:\n{}\n" +
                            "###################################################################",
                    nuconf, nunotaOrig, codUsuario,
                    itens != null ? itens.size() : 0,
                    objectMapper.writeValueAsString(req)
            );
        } catch (Exception e) {
            log.warn("Não consegui serializar FinalizarDivergenteRequest para log", e);
        }

        // LOG DETALHADO DAS QTD_CONFERIDAS
        if (itens != null && !itens.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            sb.append("\n################## DEBUG_QTD_CONFERIDA_CHEGANDO ##################\n");
            sb.append("nunotaOrig=").append(nunotaOrig)
                    .append(" nuconf=").append(nuconf)
                    .append(" totalItens=").append(itens.size()).append("\n");

            for (ItemFinalizacaoDTO it : itens) {
                sb.append(String.format(
                        "SEQ=%d CODPROD=%d QTD_CONFERIDA_RECEBIDA=%s%n",
                        it.sequencia(),
                        it.codProd(),
                        String.valueOf(it.qtdConferida())
                ));
            }
            sb.append("###############################################################");
            log.info(sb.toString());
        } else {
            log.warn("DEBUG: Requisição de finalização divergente sem itens. nuconf={}, nunotaOrig={}",
                    nuconf, nunotaOrig);
        }

        if (itens != null && !itens.isEmpty()) {
            // 1) Atualizar TGFCOI2 com QTDCONF = qtdConferida
            atualizarDetalhesConferenciaComQuantidades(nuconf, itens);

            // 2) Atualizar TGFITE.QTDNEG + VLRTOT (corte proporcional)
            atualizarItensNotaComQuantidades(nunotaOrig, itens);

            // 3) Atualizar TGFCAB.VLRNOTA com a soma dos VLRTOT já cortados
            atualizarCabecalhoNotaComNovoTotal(nunotaOrig);

            // 4) Registrar histórico no Mongo (qtdConferida e qtdAjustada)
            for (ItemFinalizacaoDTO item : itens) {
                if (item.qtdConferida() == null) {
                    log.warn("DEBUG: qtdConferida é null para item seq={} codProd={}",
                            item.sequencia(), item.codProd());
                    continue;
                }

                try {
                    log.info("📝 SALVANDO_NO_MONGO - nunota={} seq={} qtdConferida={}",
                            nunotaOrig, item.sequencia(), item.qtdConferida());

                    conferenciaItemMongoService.registrarQtdConferida(
                            nunotaOrig,
                            item.sequencia(),
                            item.qtdConferida(),
                            codUsuario != null ? codUsuario.intValue() : null
                    );

                    // Neste fluxo qtdAjustada = qtdConferida
                    conferenciaItemMongoService.registrarQtdAjustada(
                            nunotaOrig,
                            item.sequencia(),
                            item.qtdConferida(),
                            codUsuario != null ? codUsuario.intValue() : null
                    );

                    log.info("✅ MONGO_SALVO - nunota={} seq={}",
                            nunotaOrig, item.sequencia());

                } catch (Exception e) {
                    log.error("❌ ERRO_AO_SALVAR_MONGO - nunota={} seq={}",
                            nunotaOrig, item.sequencia(), e);
                }
            }
        } else {
            log.warn("Requisição de finalização divergente sem itens. nuconf={}, nunotaOrig={}",
                    nuconf, nunotaOrig);
        }

        // 5) Finalizar cabeçalho de conferência (STATUS = F)
        return finalizarCabecalhoConferenciaDivergente(nuconf, codUsuario);
    }

    // =========================================================================
    // 5.1) Atualiza TGFCOI2 (DetalhesConferencia) com QTDCONF
    // =========================================================================
    private void atualizarDetalhesConferenciaComQuantidades(
            Long nuconf,
            List<ItemFinalizacaoDTO> itens
    ) {
        ObjectNode root = objectMapper.createObjectNode();
        ObjectNode requestBody = root.putObject("requestBody");
        ObjectNode dataSet = requestBody.putObject("dataSet");

        dataSet.put("rootEntity", "DetalhesConferencia");
        dataSet.put("includePresentationFields", "S");

        ArrayNode dataRowArray = dataSet.putArray("dataRow");

        for (ItemFinalizacaoDTO item : itens) {
            if (item.qtdConferida() == null) {
                log.warn("DEBUG: qtdConferida null no TGFCOI2 - seq={}", item.sequencia());
                continue;
            }

            ObjectNode row = dataRowArray.addObject();
            ObjectNode localFields = row.putObject("localFields");

            localFields.putObject("QTDCONF")
                    .put("$", item.qtdConferida().toString());

            ObjectNode key = row.putObject("key");
            key.putObject("NUCONF").put("$", nuconf.toString());
            key.putObject("SEQCONF").put("$", item.sequencia().toString());
        }

        ObjectNode entity = dataSet.putObject("entity");
        ObjectNode fieldSet = entity.putObject("fieldSet");
        fieldSet.put("list", "NUCONF,SEQCONF,QTDCONF");

        log.info(
                "\n################## DEBUG_UPDATE_TGFCOI2 ##################\n" +
                        "nuconf={} Itens a atualizar: {}\n" +
                        "Payload:\n{}\n" +
                        "#####################################################",
                nuconf, dataRowArray.size(), root.toPrettyString()
        );

        JsonNode response = gatewayClient.callService("CRUDServiceProvider.saveRecord", root);
        log.info("✅ TGFCOI2_ATUALIZADO - Resposta: {}", response != null ? "SUCESSO" : "FALHA");
    }

    // =========================================================================
    // 5.2) Atualiza TGFITE.QTDNEG e VLRTOT de forma PROPORCIONAL
    //      usando os totais originais do item
    // =========================================================================
    private void atualizarItensNotaComQuantidades(
            Long nunotaOrig,
            List<ItemFinalizacaoDTO> itens
    ) {
        ObjectNode root = objectMapper.createObjectNode();
        ObjectNode requestBody = root.putObject("requestBody");
        ObjectNode dataSet = requestBody.putObject("dataSet");

        dataSet.put("rootEntity", "ItemNota");
        dataSet.put("includePresentationFields", "S");

        ArrayNode dataRowArray = dataSet.putArray("dataRow");

        for (ItemFinalizacaoDTO item : itens) {
            if (item.qtdConferida() == null) {
                log.warn("DEBUG: qtdConferida null no TGFITE - seq={}", item.sequencia());
                continue;
            }

            Double qtdConf = item.qtdConferida();

            // Busca VLRTOT e QTDNEG originais na TGFITE para este item
            TotaisItem totaisOrig = buscarTotaisOriginaisItem(
                    nunotaOrig,
                    item.sequencia(),
                    item.codProd()
            );

            double vlrTotOriginal = totaisOrig.vlrTotOriginal();
            double qtdOriginal    = totaisOrig.qtdOriginal();

            if (vlrTotOriginal == 0.0 || qtdOriginal <= 0.0) {
                log.warn(
                        "[atualizarItensNotaComQuantidades] Totais originais inválidos. " +
                                "NUNOTA={}, SEQ={}, CODPROD={}, vlrTotOriginal={}, qtdOriginal={}",
                        nunotaOrig, item.sequencia(), item.codProd(), vlrTotOriginal, qtdOriginal
                );
                // fallback: se não achar nada, não mexe nesse item
                continue;
            }

            // Fator de corte: ex. 10 -> 7 peças => fator = 0,7
            double fator      = qtdConf / qtdOriginal;
            double novoVlrTot = round(vlrTotOriginal * fator, 2);

            String qtdConfStr = String.format(Locale.US, "%.4f", qtdConf);
            String vlrTotStr  = String.format(Locale.US, "%.2f", novoVlrTot);

            log.info(
                    "[atualizarItensNotaComQuantidades] Corte proporcional. NUNOTA={}, SEQ={}, CODPROD={}, " +
                            "qtdOrig={}, qtdConf={}, fator={}, vlrTotOrig={}, novoVLRTOT={}",
                    nunotaOrig, item.sequencia(), item.codProd(),
                    qtdOriginal, qtdConf, fator, vlrTotOriginal, novoVlrTot
            );

            ObjectNode row = dataRowArray.addObject();
            ObjectNode localFields = row.putObject("localFields");

            // Atualiza somente QTDNEG e VLRTOT
            localFields.putObject("QTDNEG").put("$", qtdConfStr);
            localFields.putObject("VLRTOT").put("$", vlrTotStr);

            ObjectNode key = row.putObject("key");
            key.putObject("NUNOTA").put("$", nunotaOrig.toString());
            key.putObject("SEQUENCIA").put("$", item.sequencia().toString());
        }

        ObjectNode entity = dataSet.putObject("entity");
        ObjectNode fieldSet = entity.putObject("fieldSet");
        fieldSet.put("list", "NUNOTA,SEQUENCIA,QTDNEG,VLRTOT");

        log.info(
                "\n################## DEBUG_UPDATE_TGFITE ##################\n" +
                        "nunotaOrig={} Itens a atualizar: {}\n" +
                        "Payload:\n{}\n" +
                        "###################################################",
                nunotaOrig, dataRowArray.size(), root.toPrettyString()
        );

        JsonNode response = gatewayClient.callService("CRUDServiceProvider.saveRecord", root);
        log.info("✅ TGFITE_ATUALIZADO - Resposta: {}", response != null ? "SUCESSO" : "FALHA");
    }

    // =========================================================================
    // 5.3) Atualiza TGFCAB.VLRNOTA somando VLRTOT dos itens (já cortados)
    // =========================================================================
    private void atualizarCabecalhoNotaComNovoTotal(Long nunotaOrig) {
        double novoVlrNota = calcularNovoVlrNota(nunotaOrig);

        String vlrNotaStr = String.format(Locale.US, "%.2f", novoVlrNota);

        log.info(
                "[atualizarCabecalhoNotaComNovoTotal] Atualizando VLRNOTA. NUNOTA={} novoVlrNota={}",
                nunotaOrig, vlrNotaStr
        );

        ObjectNode root = objectMapper.createObjectNode();
        ObjectNode requestBody = root.putObject("requestBody");
        ObjectNode dataSet = requestBody.putObject("dataSet");

        dataSet.put("rootEntity", "CabecalhoNota");
        dataSet.put("includePresentationFields", "S");

        ObjectNode dataRow = dataSet.putObject("dataRow");
        ObjectNode localFields = dataRow.putObject("localFields");

        localFields.putObject("VLRNOTA").put("$", vlrNotaStr);

        ObjectNode key = dataRow.putObject("key");
        key.putObject("NUNOTA").put("$", nunotaOrig.toString());

        ObjectNode entity = dataSet.putObject("entity");
        ObjectNode fieldSet = entity.putObject("fieldSet");
        fieldSet.put("list", "NUNOTA,VLRNOTA");

        log.info(
                "\n################## DEBUG_UPDATE_TGFCAB_VLRNOTA ##################\n" +
                        "nunotaOrig={} Payload:\n{}\n" +
                        "############################################################",
                nunotaOrig, root.toPrettyString()
        );

        JsonNode response = gatewayClient.callService("CRUDServiceProvider.saveRecord", root);
        log.info("✅ TGFCAB_VLRNOTA_ATUALIZADO - Resposta: {}", response != null ? "SUCESSO" : "FALHA");
    }

    // Soma o VLRTOT de todos os itens já cortados da nota
    private double calcularNovoVlrNota(Long nunotaOrig) {
        String sql = """
        SELECT SUM(VLRTOT) AS NOVO_VLRNOTA
          FROM TGFITE
         WHERE NUNOTA = %d
        """.formatted(nunotaOrig);

        JsonNode root = gatewayClient.executeDbExplorer(sql);
        JsonNode responseBody = root.path("responseBody");
        JsonNode rows = responseBody.path("rows");
        JsonNode fieldsMetadata = responseBody.path("fieldsMetadata");

        if (!rows.isArray() || rows.size() == 0) {
            log.warn("[calcularNovoVlrNota] Nenhum item encontrado para somar. NUNOTA={}", nunotaOrig);
            return 0.0;
        }

        List<String> cols = extractColumns(fieldsMetadata);
        int idxTotal = indexOf(cols, "NOVO_VLRNOTA");
        if (idxTotal < 0) {
            idxTotal = 0; // fallback
        }

        JsonNode firstRow = rows.get(0);
        String totalStr = readText(firstRow, idxTotal);

        if (totalStr == null || totalStr.isBlank()) {
            log.warn("[calcularNovoVlrNota] Total nulo/vazio. NUNOTA={}", nunotaOrig);
            return 0.0;
        }

        try {
            double total = Double.parseDouble(totalStr);
            log.info("[calcularNovoVlrNota] NUNOTA={} totalItens={}", nunotaOrig, total);
            return total;
        } catch (NumberFormatException e) {
            log.error("[calcularNovoVlrNota] Erro ao converter total='{}' para double. NUNOTA={}",
                    totalStr, nunotaOrig, e);
            return 0.0;
        }
    }

    // =========================================================================
    // 6) Finalizar cabeçalho de conferência divergente (STATUS = F)
    // =========================================================================
    private JsonNode finalizarCabecalhoConferenciaDivergente(Long nuconf, Long codUsuario) {
        String agora = LocalDateTime.now().format(FMT);

        ObjectNode root = objectMapper.createObjectNode();
        ObjectNode requestBody = root.putObject("requestBody");
        ObjectNode dataSet = requestBody.putObject("dataSet");

        dataSet.put("rootEntity", "CabecalhoConferencia");
        dataSet.put("includePresentationFields", "S");

        ObjectNode dataRow = dataSet.putObject("dataRow");
        ObjectNode localFields = dataRow.putObject("localFields");

        localFields.putObject("STATUS").put("$", "F");
        localFields.putObject("DHFINCONF").put("$", agora);
        localFields.putObject("CODUSUCONF").put("$", codUsuario.toString());

        ObjectNode key = dataRow.putObject("key");
        key.putObject("NUCONF").put("$", nuconf.toString());

        ObjectNode entity = dataSet.putObject("entity");
        ObjectNode fieldSet = entity.putObject("fieldSet");
        fieldSet.put("list",
                "NUCONF,NUNOTAORIG,STATUS,DHINICONF,DHFINCONF,CODUSUCONF"
        );

        log.info(
                "\n################## DEBUG_FINALIZA_CABECALHO ##################\n" +
                        "nuconf={} PAYLOAD_FINALIZA_CABECALHO:\n{}\n" +
                        "##########################################################",
                nuconf, root.toPrettyString()
        );

        return gatewayClient.callService(
                "CRUDServiceProvider.saveRecord",
                root
        );
    }

    // =========================================================================
    // 7) API auxiliar para o PedidoConferenciaService (qtd original do Mongo)
    // =========================================================================
    public Double getQtdOriginalItem(Long nunota, Integer sequencia, Double fallbackQtdAtual) {
        log.info(
                "DEBUG_QTD_ORIGINAL_GET_IN nunota={} seq={} fallbackQtdAtual={}",
                nunota, sequencia, fallbackQtdAtual
        );

        if (nunota == null || sequencia == null) {
            log.info(
                    "DEBUG_QTD_ORIGINAL_GET_NULL_KEYS nunota={} seq={} -> fallback={}",
                    nunota, sequencia, fallbackQtdAtual
            );
            return fallbackQtdAtual;
        }

        return conferenciaItemMongoService
                .buscar(nunota, sequencia)
                .map(ItemConferenciaDoc::getQtdOriginal)
                .map(q -> {
                    log.info("DEBUG_QTD_ORIGINAL_GET_FOUND nunota={} seq={} qtdOriginal={}",
                            nunota, sequencia, q);
                    return q;
                })
                .orElseGet(() -> {
                    log.info(
                            "DEBUG_QTD_ORIGINAL_GET_NOT_FOUND nunota={} seq={} -> fallback={}",
                            nunota, sequencia, fallbackQtdAtual
                    );
                    return fallbackQtdAtual;
                });
    }

    // =========================================================================
    // 8) Totais originais por item (VLRTOT + QTDNEG) para cálculo proporcional
    // =========================================================================
    private record TotaisItem(double vlrTotOriginal, double qtdOriginal) {}

    // Busca VLRTOT e QTDNEG originais do item na TGFITE
    private TotaisItem buscarTotaisOriginaisItem(
            Long nunotaOrig,
            Integer sequencia,
            Integer codProd
    ) {
        String sql = """
        SELECT VLRTOT, QTDNEG
          FROM TGFITE
         WHERE NUNOTA   = %d
           AND SEQUENCIA = %d
           AND CODPROD   = %d
        """.formatted(nunotaOrig, sequencia, codProd);

        JsonNode root = gatewayClient.executeDbExplorer(sql);
        JsonNode responseBody = root.path("responseBody");
        JsonNode rows = responseBody.path("rows");
        JsonNode fieldsMetadata = responseBody.path("fieldsMetadata");

        if (!rows.isArray() || rows.size() == 0) {
            log.warn("[buscarTotaisOriginaisItem] Nenhum item encontrado. NUNOTA={}, SEQ={}, CODPROD={}",
                    nunotaOrig, sequencia, codProd);
            return new TotaisItem(0.0, 0.0);
        }

        List<String> cols = extractColumns(fieldsMetadata);
        int idxVlrTot  = indexOf(cols, "VLRTOT");
        int idxQtdNeg  = indexOf(cols, "QTDNEG");

        JsonNode firstRow = rows.get(0);

        String vlrTotStr = (idxVlrTot >= 0) ? readText(firstRow, idxVlrTot) : null;
        String qtdNegStr = (idxQtdNeg >= 0) ? readText(firstRow, idxQtdNeg) : null;

        double vlrTotOriginal = 0.0;
        double qtdOriginal    = 0.0;

        try {
            if (vlrTotStr != null && !vlrTotStr.isBlank()) {
                vlrTotOriginal = Double.parseDouble(vlrTotStr);
            }
        } catch (NumberFormatException e) {
            log.error("[buscarTotaisOriginaisItem] Erro ao converter VLRTOT='{}' para double. NUNOTA={}, SEQ={}, CODPROD={}",
                    vlrTotStr, nunotaOrig, sequencia, codProd, e);
        }

        try {
            if (qtdNegStr != null && !qtdNegStr.isBlank()) {
                qtdOriginal = Double.parseDouble(qtdNegStr);
            }
        } catch (NumberFormatException e) {
            log.error("[buscarTotaisOriginaisItem] Erro ao converter QTDNEG='{}' para double. NUNOTA={}, SEQ={}, CODPROD={}",
                    qtdNegStr, nunotaOrig, sequencia, codProd, e);
        }

        log.info(
                "[buscarTotaisOriginaisItem] NUNOTA={}, SEQ={}, CODPROD={}, VLRTOT_ORIG={}, QTD_ORIG={}",
                nunotaOrig, sequencia, codProd, vlrTotOriginal, qtdOriginal
        );

        return new TotaisItem(vlrTotOriginal, qtdOriginal);
    }

    // =========================================================================
    // 9) HELPERS GENÉRICOS
    // =========================================================================
    private static List<String> extractColumns(JsonNode fieldsMetadata) {
        List<String> cols = new ArrayList<>();
        if (fieldsMetadata != null && fieldsMetadata.isArray()) {
            for (JsonNode f : fieldsMetadata) {
                cols.add(f.path("name").asText(""));
            }
        }
        return cols;
    }

    private static int indexOf(List<String> cols, String name) {
        for (int i = 0; i < cols.size(); i++) {
            if (name.equalsIgnoreCase(cols.get(i))) return i;
        }
        return -1;
    }

    private static String readText(JsonNode row, int idx) {
        if (idx < 0 || idx >= row.size()) return null;
        JsonNode v = row.get(idx);
        return v.isNull() ? null : v.asText(null);
    }

    private static double round(double value, int scale) {
        if (scale < 0) return value;
        double factor = Math.pow(10, scale);
        return Math.round(value * factor) / factor;
    }
}
