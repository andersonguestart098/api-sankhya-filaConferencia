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

        ObjectNode root = objectMapper.createObjectNode();
        ObjectNode requestBody = root.putObject("requestBody");
        ObjectNode dataSet = requestBody.putObject("dataSet");

        dataSet.put("rootEntity", "CabecalhoConferencia");
        dataSet.put("includePresentationFields", "S");

        ObjectNode dataRow = dataSet.putObject("dataRow");
        ObjectNode localFields = dataRow.putObject("localFields");

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
    // 3) COPIAR ITENS DA TGFITE PARA TGFCOI2 + REGISTRAR QTD ORIGINAL NO MONGO
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
    // 4) FINALIZAR CONFERÊNCIA NORMAL (sem corte)
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
    // =========================================================================
    public JsonNode finalizarConferenciaDivergente(FinalizarDivergenteRequest req) {
        Long nuconf = req.nuconf();
        Long nunotaOrig = req.nunotaOrig();
        Long codUsuario = req.codUsuario();
        List<ItemFinalizacaoDTO> itens = req.itens();

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
            // 1) Atualiza TGFCOI2 com QTDCONF
            atualizarDetalhesConferenciaComQuantidades(nuconf, itens);

            // 2) Atualiza SOMENTE QTDCORTE no ItemNota (pedido)
            atualizarItensNotaComQtdCorte(nunotaOrig, itens);

            // 3) Mongo – histórico
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
        }

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
    // 5.2) Atualiza SOMENTE QTDCORTE no ItemNota (pedido)
    //      Qtd. corte = qtdOriginal (Mongo) - qtdConferida
    // =========================================================================
    private void atualizarItensNotaComQtdCorte(
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
                log.warn("DEBUG: qtdConferida null no QTDCORTE - seq={}", item.sequencia());
                continue;
            }

            Double qtdConf = item.qtdConferida();

            // Busca qtd original do Mongo (ou usa a própria qtdConf como fallback)
            Double qtdOriginalObj = getQtdOriginalItem(
                    nunotaOrig,
                    item.sequencia(),
                    qtdConf
            );

            if (qtdOriginalObj == null || qtdOriginalObj <= 0.0) {
                log.warn(
                        "[atualizarItensNotaComQtdCorte] QTD original inválida. " +
                                "NUNOTA={}, SEQ={}, CODPROD={}, qtdOriginal={}",
                        nunotaOrig, item.sequencia(), item.codProd(), qtdOriginalObj
                );
                continue;
            }

            double qtdOriginal = qtdOriginalObj;
            double qtdCorte = qtdOriginal - qtdConf;

            if (qtdCorte <= 0.0) {
                log.info(
                        "[atualizarItensNotaComQtdCorte] Sem corte (qtdCorte<=0). NUNOTA={}, SEQ={}, CODPROD={}, qtdOrig={}, qtdConf={}",
                        nunotaOrig, item.sequencia(), item.codProd(), qtdOriginal, qtdConf
                );
                continue;
            }

            String qtdCorteStr = String.format(Locale.US, "%.4f", qtdCorte);

            log.info(
                    "[atualizarItensNotaComQtdCorte] NUNOTA={}, SEQ={}, CODPROD={}, qtdOrig={}, qtdConf={}, qtdCorte={}",
                    nunotaOrig, item.sequencia(), item.codProd(),
                    qtdOriginal, qtdConf, qtdCorte
            );

            ObjectNode row = dataRowArray.addObject();
            ObjectNode localFields = row.putObject("localFields");

            // ⚠️ Campo da "Qtd. corte" no pedido
            localFields.putObject("QTDCORTE").put("$", qtdCorteStr);

            ObjectNode key = row.putObject("key");
            key.putObject("NUNOTA").put("$", nunotaOrig.toString());
            key.putObject("SEQUENCIA").put("$", item.sequencia().toString());
        }

        ObjectNode entity = dataSet.putObject("entity");
        ObjectNode fieldSet = entity.putObject("fieldSet");
        fieldSet.put("list", "NUNOTA,SEQUENCIA,QTDCORTE");

        log.info(
                "\n################## DEBUG_UPDATE_TGFITE_QTDCORTE ##################\n" +
                        "nunotaOrig={} Itens a atualizar: {}\n" +
                        "Payload:\n{}\n" +
                        "###################################################",
                nunotaOrig, dataRowArray.size(), root.toPrettyString()
        );

        JsonNode response = gatewayClient.callService("CRUDServiceProvider.saveRecord", root);
        log.info("✅ TGFITE_QTDCORTE_ATUALIZADO - Resposta: {}", response != null ? "SUCESSO" : "FALHA");
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
    // 7) API auxiliar – qtd original do Mongo
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
    // 8) HELPERS
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
}
