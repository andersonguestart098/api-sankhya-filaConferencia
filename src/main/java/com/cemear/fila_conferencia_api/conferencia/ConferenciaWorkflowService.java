// src/main/java/com/cemear/fila_conferencia_api/conferencia/ConferenciaWorkflowService.java
package com.cemear.fila_conferencia_api.conferencia;

import com.cemear.fila_conferencia_api.auth.dto.FinalizarDivergenteRequest;
import com.cemear.fila_conferencia_api.auth.dto.FinalizarDivergenteRequest.ItemFinalizacaoDTO;
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
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
@RequiredArgsConstructor
public class ConferenciaWorkflowService {

    private final SankhyaGatewayClient gatewayClient;
    private final ObjectMapper objectMapper;

    private static final DateTimeFormatter FMT =
            DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm");

    /**
     * Mapa em memória:
     *  - chave externa: NUNOTA (nota de origem)
     *  - chave interna: SEQUENCIA do item
     *  - valor: QTDNEG original (antes de qualquer corte)
     */
    private final Map<Long, Map<Integer, Double>> qtdOriginalPorNota = new ConcurrentHashMap<>();

    // ---------------------------------
    // INICIAR CONFERÊNCIA (CabecalhoConferencia)
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
        // aqui você já usava "fieldset" assim; mantive como está
        ObjectNode fieldSet = entity.putObject("fieldset");
        fieldSet.put("list", "NUNOTA,NUCONFATUAL");

        gatewayClient.callService("CRUDServiceProvider.saveRecord", root);
    }

    // ------------------------------------------------------
    // COPIAR ITENS DA TGFITE PARA TGFCOI2 (DetalhesConferencia)
    // + GUARDAR QTDNEG ORIGINAL EM MEMÓRIA
    // ------------------------------------------------------
    public JsonNode preencherItensConferencia(Long nunotaOrig, Long nuconf) {

        log.info("########## DEBUG_QTD_ORIGINAL_INICIAR_PREENCIMENTO nunotaOrig={} nuconf={} ##########",
                nunotaOrig, nuconf);

        // 1) Busca itens do pedido na TGFITE via DbExplorer
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

        log.info("DEBUG_QTD_ORIGINAL_SQL_TGFITE nunotaOrig={} SQL:\n{}", nunotaOrig, sql);

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

        log.info("DEBUG_QTD_ORIGINAL_ROWS nunotaOrig={} totalLinhas={}", nunotaOrig, rows.size());
        log.info("DEBUG_QTD_ORIGINAL_FIELDS nunotaOrig={} fields={}", nunotaOrig, fieldsMetadata.toPrettyString());

        List<String> cols = extractColumns(fieldsMetadata);

        int iSequencia = indexOf(cols, "SEQUENCIA");
        int iCodprod   = indexOf(cols, "CODPROD");
        int iCodvol    = indexOf(cols, "CODVOL");
        int iControle  = indexOf(cols, "CONTROLE");
        int iQtdneg    = indexOf(cols, "QTDNEG");

        // mapa temporário para essa nota
        Map<Integer, Double> mapaQtdOriginal = new HashMap<>();

        // 2) Monta o body do DetalhesConferencia com dataRow[]
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
            Integer seqInt = null;

            if (seqConf == null || seqConf.isBlank()) {
                seqInt = count + 1;
                seqConf = String.valueOf(seqInt);
            } else {
                try {
                    seqInt = Integer.valueOf(seqConf);
                } catch (NumberFormatException e) {
                    // se vier zoado, usa contador como fallback
                    seqInt = count + 1;
                    seqConf = String.valueOf(seqInt);
                }
            }

            String codprod  = readText(r, iCodprod);
            String codvol   = readText(r, iCodvol);
            String controle = readText(r, iControle);
            String qtdneg   = readText(r, iQtdneg);

            log.info(
                    "DEBUG_QTD_ORIGINAL_ROW nunota={} seqRaw='{}' seqInt={} codProd='{}' codvol='{}' controle='{}' qtdneg='{}'",
                    nunotaOrig, seqConf, seqInt, codprod, codvol, controle, qtdneg
            );

            // guarda QTDNEG original em memória
            if (seqInt != null && qtdneg != null && !qtdneg.isBlank()) {
                try {
                    Double qtdOriginal = Double.valueOf(qtdneg);
                    mapaQtdOriginal.put(seqInt, qtdOriginal);

                    log.info(
                            "DEBUG_QTD_ORIGINAL_STORE_ITEM nunota={} seq={} qtdOriginal={} (qtdnegBruto='{}')",
                            nunotaOrig, seqInt, qtdOriginal, qtdneg
                    );
                } catch (NumberFormatException e) {
                    log.warn(
                            "Não consegui converter QTDNEG para Double. nunota={}, seq={}, qtdneg='{}'",
                            nunotaOrig, seqInt, qtdneg
                    );
                }
            } else {
                log.info(
                        "DEBUG_QTD_ORIGINAL_SKIP_ITEM nunota={} seqInt={} qtdneg='{}' (não armazenado)",
                        nunotaOrig, seqInt, qtdneg
                );
            }

            ObjectNode rowNode = dataRowArray.addObject();
            ObjectNode local = rowNode.putObject("localFields");

            // IMPORTANTE: aqui você continua preenchendo QTDCONFVOLPAD com a QTDNEG original
            local.putObject("NUCONF").put("$", nuconf.toString());
            local.putObject("SEQCONF").put("$", seqConf);
            local.putObject("CODBARRA").put("$", "");
            local.putObject("CODPROD").put("$", codprod);
            local.putObject("CODVOL").put("$", codvol);
            local.putObject("CONTROLE").put("$", controle == null ? "" : controle);
            local.putObject("QTDCONFVOLPAD").put("$", qtdneg); // QTD ESPERADA (original)
            local.putObject("QTDCONF").put("$", qtdneg);       // inicia igual à esperada
            local.putObject("COPIA").put("$", "N");

            count++;
        }

        // salva o mapa em memória para essa NUNOTA
        if (!mapaQtdOriginal.isEmpty()) {
            qtdOriginalPorNota.put(nunotaOrig, mapaQtdOriginal);
            log.info("DEBUG_QTD_ORIGINAL_STORE_MAP nunota={} -> {}", nunotaOrig, mapaQtdOriginal);
        } else {
            log.info("DEBUG_QTD_ORIGINAL_STORE_MAP nunota={} - nenhum QTDNEG armazenado", nunotaOrig);
        }

        ObjectNode entity = dataSet.putObject("entity");
        ObjectNode fieldSet = entity.putObject("fieldSet");
        fieldSet.put("list",
                "NUCONF,SEQCONF,CODBARRA,CODPROD,CODVOL,CONTROLE,QTDCONFVOLPAD,QTDCONF,COPIA"
        );

        log.info("DEBUG_QTD_ORIGINAL_SAVE_DETALHES_REQUEST nunota={} payload:\n{}",
                nunotaOrig, rootReq.toPrettyString());

        return gatewayClient.callService(
                "CRUDServiceProvider.saveRecord",
                rootReq
        );
    }

    // ---------------------------------
    // FINALIZAR CONFERÊNCIA NORMAL (STATUS = F)
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
    // FINALIZAR CONFERÊNCIA DIVERGENTE (STATUS = D)
    // Atualiza TGFCOI2 (DetalhesConferencia) e TGFITE(QTDNEG)
    // ---------------------------------
    public JsonNode finalizarConferenciaDivergente(FinalizarDivergenteRequest req) {

        Long nuconf = req.nuconf();
        Long nunotaOrig = req.nunotaOrig();
        Long codUsuario = req.codUsuario();
        List<ItemFinalizacaoDTO> itens = req.itens();

        // LOG ESCANDALOSO DA REQ QUE VEM DO APP
        try {
            log.info(
                    "\n################## DEBUG_FINALIZAR_DIVERGENTE_REQ ##################\n" +
                            "nuconf={} nunotaOrig={} codUsuario={}\n" +
                            "PAYLOAD_COMPLETO_DO_APP:\n{}\n" +
                            "###################################################################",
                    nuconf, nunotaOrig, codUsuario,
                    objectMapper.writeValueAsString(req)
            );
        } catch (Exception e) {
            log.warn("Não consegui serializar FinalizarDivergenteRequest para log", e);
        }

        log.info("DEBUG_DIVERGENTE_INICIO nuconf={} nunotaOrig={} codUsuario={} totalItens={}",
                nuconf, nunotaOrig, codUsuario, itens != null ? itens.size() : 0);

        if (itens != null) {
            for (ItemFinalizacaoDTO it : itens) {
                log.info(
                        "################## AQUI_QTD_CONFERIDA_DO_APP ##################\n" +
                                "nunotaOrig={} nuconf={} seq={} codProd={} qtdConferida={}\n" +
                                "###############################################################",
                        nunotaOrig, nuconf, it.sequencia(), it.codProd(), it.qtdConferida()
                );
            }
        }

        if (itens == null || itens.isEmpty()) {
            log.warn("Requisição de finalização divergente sem itens. nuconf={}, nunotaOrig={}",
                    nuconf, nunotaOrig);
        } else {
            // 1) Atualizar TGFCOI2 (DetalhesConferencia) com QTDCONF = qtdConferida
            atualizarDetalhesConferenciaComQuantidades(nuconf, itens);

            // 2) Atualizar TGFITE.QTDNEG = qtdConferida
            atualizarItensNotaComQuantidades(nunotaOrig, itens);
        }

        // 3) Finalizar cabeçalho com STATUS = D (aqui mantive F, como estava no seu código)
        return finalizarCabecalhoConferenciaDivergente(nuconf, codUsuario);
    }

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
            if (item.qtdConferida() == null) continue;

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
                "\n################## DEBUG_DIVERGENTE_UPDATE_TGFCOI2 ##################\n" +
                        "nuconf={} PAYLOAD_ENVIADO_PARA_TGFCOI2:\n{}\n" +
                        "#################################################################",
                nuconf, root.toPrettyString()
        );

        gatewayClient.callService("CRUDServiceProvider.saveRecord", root);
    }

    // atualiza TGFITE.QTDNEG = qtdConferida
    private void atualizarItensNotaComQuantidades(
            Long nunotaOrig,
            List<ItemFinalizacaoDTO> itens
    ) {
        ObjectNode root = objectMapper.createObjectNode();
        ObjectNode requestBody = root.putObject("requestBody");
        ObjectNode dataSet = requestBody.putObject("dataSet");

        dataSet.put("rootEntity", "ItemNota"); // entidade padrão de item (TGFITE)
        dataSet.put("includePresentationFields", "S");

        ArrayNode dataRowArray = dataSet.putArray("dataRow");

        for (ItemFinalizacaoDTO item : itens) {
            if (item.qtdConferida() == null) continue;

            ObjectNode row = dataRowArray.addObject();
            ObjectNode localFields = row.putObject("localFields");

            localFields.putObject("QTDNEG")
                    .put("$", item.qtdConferida().toString());

            ObjectNode key = row.putObject("key");
            key.putObject("NUNOTA").put("$", nunotaOrig.toString());
            key.putObject("SEQUENCIA").put("$", item.sequencia().toString());
        }

        ObjectNode entity = dataSet.putObject("entity");
        ObjectNode fieldSet = entity.putObject("fieldSet");
        fieldSet.put("list", "NUNOTA,SEQUENCIA,QTDNEG");

        log.info(
                "\n################## DEBUG_DIVERGENTE_UPDATE_TGFITE ##################\n" +
                        "nunotaOrig={} PAYLOAD_ENVIADO_PARA_TGFITE:\n{}\n" +
                        "################################################################",
                nunotaOrig, root.toPrettyString()
        );

        gatewayClient.callService("CRUDServiceProvider.saveRecord", root);
    }

    // finaliza cabeçalho com STATUS = D (mas STATUS = 'F' como estava no seu código)
    private JsonNode finalizarCabecalhoConferenciaDivergente(Long nuconf, Long codUsuario) {
        String agora = LocalDateTime.now().format(FMT);

        ObjectNode root = objectMapper.createObjectNode();
        ObjectNode requestBody = root.putObject("requestBody");
        ObjectNode dataSet = requestBody.putObject("dataSet");

        dataSet.put("rootEntity", "CabecalhoConferencia");
        dataSet.put("includePresentationFields", "S");

        ObjectNode dataRow = dataSet.putObject("dataRow");
        ObjectNode localFields = dataRow.putObject("localFields");

        // aqui você havia deixado STATUS = 'F'; mantive igual
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
                "\n################## DEBUG_DIVERGENTE_FINALIZA_CAB ##################\n" +
                        "nuconf={} PAYLOAD_FINALIZA_CABECALHO:\n{}\n" +
                        "################################################################",
                nuconf, root.toPrettyString()
        );

        return gatewayClient.callService(
                "CRUDServiceProvider.saveRecord",
                root
        );
    }

    // ----------------- API para o PedidoConferenciaService -----------------

    /**
     * Retorna a quantidade original (antes de cortes) para um item da nota.
     * Se não encontrar na memória, devolve o fallback (que normalmente é a QTD_ATUAL).
     */
    public Double getQtdOriginalItem(Long nunota, Integer sequencia, Double fallbackQtdAtual) {

        log.info(
                "DEBUG_QTD_ORIGINAL_GET_IN nunota={} seq={} fallbackQtdAtual={}",
                nunota, sequencia, fallbackQtdAtual
        );

        if (nunota == null || sequencia == null) {
            log.info(
                    "DEBUG_QTD_ORIGINAL_GET_NULL_KEYS nunota={} seq={} -> retornando fallback={}",
                    nunota, sequencia, fallbackQtdAtual
            );
            return fallbackQtdAtual;
        }

        Map<Integer, Double> mapa = qtdOriginalPorNota.get(nunota);

        if (mapa == null) {
            log.info(
                    "DEBUG_QTD_ORIGINAL_GET_MAP_NULL nunota={} seq={} -> mapa nulo, retornando fallback={}",
                    nunota, sequencia, fallbackQtdAtual
            );
            return fallbackQtdAtual;
        }

        Double qtdOriginal = mapa.get(sequencia);

        if (qtdOriginal == null) {
            log.info(
                    "DEBUG_QTD_ORIGINAL_GET_NOT_FOUND nunota={} seq={} mapa={} -> sem entrada, retornando fallback={}",
                    nunota, sequencia, mapa, fallbackQtdAtual
            );
            return fallbackQtdAtual;
        }

        log.info(
                "DEBUG_QTD_ORIGINAL_GET_FOUND nunota={} seq={} qtdOriginal={} mapa={}",
                nunota, sequencia, qtdOriginal, mapa
        );

        return qtdOriginal;
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
