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

    // ---------------------------------
    // INICIAR CONFERÊNCIA (CabecalhoConferencia)
    // NOVO FLUXO: APENAS CABEÇALHO (STATUS = A)
    // ---------------------------------
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

    // ---------------------------------
    // ATUALIZAR NUCONF NO CABEÇALHO DA NOTA (TGFCAB)
    // ---------------------------------
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

    // ---------------------------------
    // Verificar se NUCONF está finalizado OK (STATUS = 'F')
    // ---------------------------------
    private boolean conferenciaEstaFinalizadaOk(Long nuconf) {
        if (nuconf == null) {
            return false;
        }

        String sql = """
            SELECT STATUS
              FROM TGFCON2
             WHERE NUCONF = %d
            """.formatted(nuconf);

        JsonNode root = gatewayClient.executeDbExplorer(sql);
        JsonNode responseBody = root.path("responseBody");
        JsonNode fieldsMetadata = responseBody.path("fieldsMetadata");
        JsonNode rows = responseBody.path("rows");

        if (!rows.isArray() || rows.size() == 0) {
            log.warn("[conferenciaEstaFinalizadaOk] Nenhum cabeçalho encontrado. NUCONF={}", nuconf);
            return false;
        }

        List<String> cols = extractColumns(fieldsMetadata);
        int idxStatus = indexOf(cols, "STATUS");
        if (idxStatus < 0) {
            log.warn("[conferenciaEstaFinalizadaOk] Coluna STATUS não encontrada no metadata. NUCONF={}", nuconf);
            return false;
        }

        String status = readText(rows.get(0), idxStatus);
        log.info("[conferenciaEstaFinalizadaOk] NUCONF={} STATUS_LIDO={}", nuconf, status);

        return "F".equalsIgnoreCase(status);
    }

    // ------------------------------------------------------
    // COPIAR ITENS DA TGFITE PARA TGFCOI2 (DetalhesConferencia)
    // + REGISTRAR QTDNEG ORIGINAL NO MONGO
    //
    // NOVO FLUXO: ESTE MÉTODO DEVE SER CHAMADO
    // APENAS QUANDO A CONFERÊNCIA FOR FINALIZADA OK.
    // ------------------------------------------------------
    public JsonNode preencherItensConferencia(Long nunotaOrig, Long nuconf) {
        log.info("########## DEBUG_PREENCHE_ITENS nunotaOrig={} nuconf={} ##########",
                nunotaOrig, nuconf);

        // 🔒 Segurança: só preenche itens se NUCONF estiver com STATUS = 'F'
        if (!conferenciaEstaFinalizadaOk(nuconf)) {
            log.warn("preencherItensConferencia chamado para NUCONF={} com STATUS != 'F'. " +
                    "Nenhum item será gravado em TGFCOI2.", nuconf);

            ObjectNode ignorado = objectMapper.createObjectNode();
            ignorado.put("status", "IGNORADO");
            ignorado.put("motivo", "Conferência não está finalizada OK (STATUS != 'F')");
            ignorado.put("nunotaOrig", nunotaOrig);
            ignorado.put("nuconf", nuconf);
            return ignorado;
        }

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

            // REGISTRA SNAPSHOT ORIGINAL NO MONGO
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

    // ---------------------------------
    // FINALIZAR CONFERÊNCIA NORMAL (STATUS = F)
    //
    // OBS: ESTE MÉTODO APENAS FINALIZA O CABEÇALHO.
    // PARA O NOVO FLUXO (PREENCHER ITENS + FINALIZAR),
    // USE O MÉTODO finalizarConferenciaOkComItens() LOGO ABAIXO.
    // ---------------------------------
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

    // ---------------------------------
    // NOVO FLUXO OK:
    // 1) FINALIZA CABEÇALHO (STATUS = F)
    // 2) COPIA ITENS (TGFITE -> TGFCOI2) + REGISTRA NO MONGO
    // ---------------------------------
    public JsonNode finalizarConferenciaOkComItens(Long nunotaOrig, Long nuconf, Long codUsuario) {
        log.info("#### FINALIZAR_OK_COM_ITENS nunotaOrig={} nuconf={} codUsuario={} ####",
                nunotaOrig, nuconf, codUsuario);

        // 1) Finaliza cabeçalho (STATUS = F)
        JsonNode respCabecalho = finalizarConferencia(nuconf, codUsuario);
        log.info("FINALIZAR_OK_COM_ITENS - CabecalhoConferencia finalizado: {}",
                respCabecalho != null ? "SUCESSO" : "FALHA");

        // 2) Preenche itens de conferência (agora STATUS já é F)
        JsonNode respDetalhes = preencherItensConferencia(nunotaOrig, nuconf);
        log.info("FINALIZAR_OK_COM_ITENS - DetalhesConferencia preenchidos: {}",
                respDetalhes != null ? "SUCESSO" : "FALHA");

        // 3) Monta uma resposta combinada (opcional)
        ObjectNode combinado = objectMapper.createObjectNode();
        combinado.set("detalhesConferencia", respDetalhes);
        combinado.set("cabecalhoConferencia", respCabecalho);
        combinado.put("nunotaOrig", nunotaOrig);
        combinado.put("nuconf", nuconf);

        return combinado;
    }

    // ---------------------------------
    // FINALIZAR CONFERÊNCIA DIVERGENTE
    //
    // DEFINITIVO:
    // - NÃO ATUALIZA TGFCOI2
    // - NÃO ATUALIZA TGFITE
    // - NÃO MUDA STATUS (CABEÇALHO FICA EM "A")
    // - NÃO SALVA MAIS NEM NO MONGO
    // RESULTADO: A TELA DO SANKHYA FICA 100% "LIMPA".
    // ---------------------------------
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

        // ⚠️ A PARTIR DAQUI: NÃO MEXE EM NENHUM ITEM.
        // Nada de TGFCOI2, nada de TGFITE, nada de Mongo.
        log.info("FINALIZAR_DIVERGENTE - Nenhum item será persistido em lugar nenhum. " +
                "Somente retornando status OK para o app.");

        ObjectNode resp = objectMapper.createObjectNode();
        resp.put("status", "OK");
        resp.put("mensagem",
                "Conferência divergente registrada apenas no app. " +
                        "Nenhum item foi gerado/ajustado no Sankhya. " +
                        "Cabeçalho permanece em andamento (STATUS = A).");
        resp.put("nunotaOrig", nunotaOrig);
        resp.put("nuconf", nuconf);

        return resp;
    }

    // ----------------- MÉTODOS ANTIGOS (AINDA DISPONÍVEIS, MAS OPCIONAIS) -----------------
    // Se em algum momento você quiser voltar a automatizar corte, esses métodos continuam aqui.

    // atualiza TGFCOI2 (DetalhesConferencia)
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

    // atualiza TGFITE.QTDNEG e VLRTOT = VLRUNIT * qtdConferida
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

            // 1) Buscar VLRUNIT do item na TGFITE
            double vlrUnit = buscarVlrUnitItem(nunotaOrig, item.sequencia(), item.codProd());

            log.info(
                    "[atualizarItensNotaComQuantidades] VLRUNIT encontrado. NUNOTA={}, SEQ={}, CODPROD={}, vlrUnit={}",
                    nunotaOrig, item.sequencia(), item.codProd(), vlrUnit
            );

            // 2) Calcular novo VLRTOT
            double novoVlrTot = vlrUnit * qtdConf;

            String qtdConfStr = String.format(Locale.US, "%.4f", qtdConf);
            String vlrTotStr = String.format(Locale.US, "%.2f", novoVlrTot);

            log.info(
                    "[atualizarItensNotaComQuantidades] Aplicando corte. NUNOTA={}, SEQ={}, CODPROD={}, novaQTDNEG={}, novoVLRTOT={}",
                    nunotaOrig, item.sequencia(), item.codProd(), qtdConfStr, vlrTotStr
            );

            ObjectNode row = dataRowArray.addObject();
            ObjectNode localFields = row.putObject("localFields");

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

    // finaliza cabeçalho com STATUS = D (não usado no novo fluxo,
    // mas deixei aqui caso você queira reativar algum dia)
    private JsonNode finalizarCabecalhoConferenciaDivergente(Long nuconf, Long codUsuario) {
        String agora = LocalDateTime.now().format(FMT);

        ObjectNode root = objectMapper.createObjectNode();
        ObjectNode requestBody = root.putObject("requestBody");
        ObjectNode dataSet = requestBody.putObject("dataSet");

        dataSet.put("rootEntity", "CabecalhoConferencia");
        dataSet.put("includePresentationFields", "S");

        ObjectNode dataRow = dataSet.putObject("dataRow");
        ObjectNode localFields = dataRow.putObject("localFields");

        localFields.putObject("STATUS").put("$", "D");
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

    // ----------------- API para o PedidoConferenciaService -----------------
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

    /**
     * Busca o VLRUNIT do item na TGFITE.
     */
    private double buscarVlrUnitItem(Long nunotaOrig, Integer sequencia, Integer codProd) {
        String sql = """
        SELECT VLRUNIT
          FROM TGFITE
         WHERE NUNOTA   = %d
           AND SEQUENCIA = %d
           AND CODPROD   = %d
        """.formatted(nunotaOrig, sequencia, codProd);

        JsonNode root = gatewayClient.executeDbExplorer(sql);
        JsonNode rows = root.path("responseBody").path("rows");
        JsonNode fieldsMetadata = root.path("responseBody").path("fieldsMetadata");

        if (!rows.isArray() || rows.size() == 0) {
            log.warn("[buscarVlrUnitItem] Nenhum item encontrado. NUNOTA={}, SEQ={}, CODPROD={}",
                    nunotaOrig, sequencia, codProd);
            return 0.0;
        }

        List<String> cols = extractColumns(fieldsMetadata);
        int idxVlrUnit = indexOf(cols, "VLRUNIT");
        if (idxVlrUnit < 0) {
            log.warn("[buscarVlrUnitItem] Coluna VLRUNIT não encontrada no metadata. NUNOTA={}, SEQ={}, CODPROD={}",
                    nunotaOrig, sequencia, codProd);
            return 0.0;
        }

        JsonNode firstRow = rows.get(0);
        String vlrUnitStr = readText(firstRow, idxVlrUnit);
        if (vlrUnitStr == null || vlrUnitStr.isBlank()) {
            return 0.0;
        }

        try {
            return Double.parseDouble(vlrUnitStr);
        } catch (NumberFormatException e) {
            log.error("[buscarVlrUnitItem] Erro ao converter VLRUNIT='{}' para double. NUNOTA={}, SEQ={}, CODPROD={}",
                    vlrUnitStr, nunotaOrig, sequencia, codProd, e);
            return 0.0;
        }
    }

    // ----------------- HELPERS -----------------
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
