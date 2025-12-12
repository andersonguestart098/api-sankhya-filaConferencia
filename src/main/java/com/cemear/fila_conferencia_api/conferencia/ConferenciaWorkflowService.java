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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class ConferenciaWorkflowService {

    private final SankhyaGatewayClient gatewayClient;
    private final ObjectMapper objectMapper;
    private final ConferenciaItemMongoService conferenciaItemMongoService;

    private static final DateTimeFormatter FMT =
            DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm");

    /**
     * Item agrupado por produto/controle já na UNIDADE PADRÃO do produto.
     */
    private static class ItemAgrupadoPorProduto {
        final String codProdStr;
        final String codvolPadrao;
        final String controle;
        double qtdTotalPadrao;

        ItemAgrupadoPorProduto(String codProdStr, String codvolPadrao, String controle, double qtdInicial) {
            this.codProdStr = codProdStr;
            this.codvolPadrao = codvolPadrao;
            this.controle = controle;
            this.qtdTotalPadrao = qtdInicial;
        }

        void somar(double qtd) {
            this.qtdTotalPadrao += qtd;
        }
    }

    // Fator de conversão de cada unidade para "peça" (PC).
    // Se aparecer outra UN, é só incluir aqui ou depois buscar de tabela própria.
    private static final Map<String, Double> FATOR_PARA_PECA = Map.of(
            "PC", 1.0,
            "UN", 1.0,
            "CT", 100.0,   // cento
            "ML", 1000.0   // milheiro
    );

    private static String normalizarCodvol(String codvol) {
        return codvol == null ? null : codvol.trim().toUpperCase(Locale.ROOT);
    }

    /**
     * Converte uma quantidade da unidade do item (codvolItem)
     * para a unidade padrão do produto (codvolPadrao).
     *
     * Regra:
     * qtdItem * fator(item) / fator(padrão)
     *
     * Se não achar algum fator, devolve a quantidade original e loga um aviso.
     */
    private double converterParaUnidadePadrao(String codvolItem, String codvolPadrao, double qtd) {
        if (codvolItem == null || codvolPadrao == null) return qtd;

        String item = normalizarCodvol(codvolItem);
        String padrao = normalizarCodvol(codvolPadrao);

        if (item != null && item.equals(padrao)) return qtd;

        Double fatorItem = FATOR_PARA_PECA.get(item);
        Double fatorPadrao = FATOR_PARA_PECA.get(padrao);

        if (fatorItem != null && fatorPadrao != null) {
            double emPecas = qtd * fatorItem;
            double emPadrao = emPecas / fatorPadrao;
            log.debug("CONVERTER_QTD item={} padrao={} qtdOrig={} qtdConv={}",
                    item, padrao, qtd, emPadrao);
            return emPadrao;
        }

        log.warn("SEM_REGRA_CONVERSAO codvolItem={} codvolPadrao={} qtd={}",
                codvolItem, codvolPadrao, qtd);
        return qtd;
    }

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

        return gatewayClient.callService("CRUDServiceProvider.saveRecord", root);
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
        if (nuconf == null) return false;

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
            log.warn("STATUS_CONF_NAO_ENCONTRADO nuconf={}", nuconf);
            return false;
        }

        List<String> cols = extractColumns(fieldsMetadata);
        int idxStatus = indexOf(cols, "STATUS");
        if (idxStatus < 0) {
            log.warn("COL_STATUS_NAO_ENCONTRADA nuconf={}", nuconf);
            return false;
        }

        String status = readText(rows.get(0), idxStatus);
        log.info("STATUS_CONF nuconf={} status={}", nuconf, status);

        return "F".equalsIgnoreCase(status);
    }

    // ---------------------------------
    // Verificar se já existem itens em TGFCOI2 para o NUCONF
    // ---------------------------------
    private boolean existemItensConferencia(Long nuconf) {
        String sql = """
            SELECT COUNT(*) QTD
              FROM TGFCOI2
             WHERE NUCONF = %d
            """.formatted(nuconf);

        JsonNode root = gatewayClient.executeDbExplorer(sql);
        JsonNode body = root.path("responseBody");
        JsonNode rows = body.path("rows");
        JsonNode meta = body.path("fieldsMetadata");

        if (!rows.isArray() || rows.size() == 0) {
            return false;
        }

        List<String> cols = extractColumns(meta);
        int idxQtd = indexOf(cols, "QTD");
        if (idxQtd < 0) return false;

        String qtdStr = readText(rows.get(0), idxQtd);
        if (qtdStr == null || qtdStr.isBlank()) return false;

        try {
            long qtd = Long.parseLong(qtdStr);
            return qtd > 0;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    // ---------------------------------
    // Buscar NUNOTAORIG a partir do NUCONF em TGFCON2
    // ---------------------------------
    private Long buscarNunotaOrigPorNuconf(Long nuconf) {
        if (nuconf == null) return null;

        String sql = """
            SELECT NUNOTAORIG
              FROM TGFCON2
             WHERE NUCONF = %d
            """.formatted(nuconf);

        JsonNode root = gatewayClient.executeDbExplorer(sql);
        JsonNode responseBody = root.path("responseBody");
        JsonNode fieldsMetadata = responseBody.path("fieldsMetadata");
        JsonNode rows = responseBody.path("rows");

        if (!rows.isArray() || rows.size() == 0) {
            log.warn("NUNOTAORIG_NAO_ENCONTRADO nuconf={}", nuconf);
            return null;
        }

        List<String> cols = extractColumns(fieldsMetadata);
        int idxNunotaOrig = indexOf(cols, "NUNOTAORIG");
        if (idxNunotaOrig < 0) {
            log.warn("COL_NUNOTAORIG_NAO_ENCONTRADA nuconf={}", nuconf);
            return null;
        }

        String nunotaStr = readText(rows.get(0), idxNunotaOrig);
        if (nunotaStr == null || nunotaStr.isBlank()) {
            log.warn("NUNOTAORIG_VAZIO nuconf={}", nuconf);
            return null;
        }

        try {
            return Long.valueOf(nunotaStr);
        } catch (NumberFormatException e) {
            log.warn("NUNOTAORIG_CONVERSAO_ERRO nuconf={} valor='{}'", nuconf, nunotaStr, e);
            return null;
        }
    }

    // ------------------------------------------------------
    // COPIAR ITENS DA TGFITE PARA TGFCOI2 (DetalhesConferencia)
    // + REGISTRAR QTDNEG ORIGINAL NO MONGO
    //
    // NOVO COMPORTAMENTO:
    // - Lê CODVOLPAD (unidade padrão) de TGFPRO
    // - Converte QTDNEG da unidade do item (CODVOL) para CODVOLPAD
    //   usando FATOR_PARA_PECA
    // - AGRUPA POR (CODPROD, CONTROLE) NA UNIDADE PADRÃO
    // - GERA 1 LINHA NA TGFCOI2 POR PRODUTO
    // ------------------------------------------------------
    public JsonNode preencherItensConferencia(Long nunotaOrig, Long nuconf) {
        log.info("PREENCHER_ITENS_INICIO nunotaOrig={} nuconf={}", nunotaOrig, nuconf);

        if (!conferenciaEstaFinalizadaOk(nuconf)) {
            log.warn("PREENCHER_ITENS_CANCELADO_STATUS nuconf={}", nuconf);
            ObjectNode ignorado = objectMapper.createObjectNode();
            ignorado.put("status", "IGNORADO");
            ignorado.put("motivo", "Conferência não está finalizada (STATUS != F)");
            ignorado.put("nunotaOrig", nunotaOrig);
            ignorado.put("nuconf", nuconf);
            return ignorado;
        }

        if (existemItensConferencia(nuconf)) {
            log.warn("PREENCHER_ITENS_JA_EXISTEM nuconf={}", nuconf);
            ObjectNode ignorado = objectMapper.createObjectNode();
            ignorado.put("status", "IGNORADO_JA_EXISTEM_ITENS");
            ignorado.put("motivo", "Já existem itens em TGFCOI2 para este NUCONF");
            ignorado.put("nunotaOrig", nunotaOrig);
            ignorado.put("nuconf", nuconf);
            return ignorado;
        }

        String sql = """
            SELECT
                i.SEQUENCIA,
                i.CODPROD,
                p.DESCRPROD,
                p.CODVOLPAD,
                i.CODVOL,
                i.CONTROLE,
                i.QTDNEG
            FROM TGFITE i
            JOIN TGFPRO p ON p.CODPROD = i.CODPROD
            WHERE i.NUNOTA = %d
            """.formatted(nunotaOrig);

        log.info("PREENCHER_ITENS_SQL nunotaOrig={} len={}", nunotaOrig, sql.length());

        JsonNode root = gatewayClient.executeDbExplorer(sql);
        JsonNode responseBody = root.path("responseBody");
        JsonNode fieldsMetadata = responseBody.path("fieldsMetadata");
        JsonNode rowsNode = responseBody.path("rows");

        if (!rowsNode.isArray()) {
            log.error("PREENCHER_ITENS_ROWS_INVALID nunotaOrig={} tipo={}",
                    nunotaOrig, rowsNode.getNodeType());
            throw new IllegalStateException("DbExplorer não retornou array 'rows'.");
        }

        ArrayNode rows = (ArrayNode) rowsNode;
        List<String> cols = extractColumns(fieldsMetadata);

        int iSequencia   = indexOf(cols, "SEQUENCIA");
        int iCodprod     = indexOf(cols, "CODPROD");
        int iCodvolPad   = indexOf(cols, "CODVOLPAD");
        int iCodvolItem  = indexOf(cols, "CODVOL");
        int iControle    = indexOf(cols, "CONTROLE");
        int iQtdneg      = indexOf(cols, "QTDNEG");

        Map<String, ItemAgrupadoPorProduto> agrupados = new LinkedHashMap<>();

        int count = 0;
        for (JsonNode r : rows) {
            if (!r.isArray()) continue;

            String seqStr = readText(r, iSequencia);
            Integer seqInt;
            if (seqStr == null || seqStr.isBlank()) {
                seqInt = count + 1;
            } else {
                try {
                    seqInt = Integer.valueOf(seqStr);
                } catch (NumberFormatException e) {
                    seqInt = count + 1;
                }
            }

            String codprodStr    = readText(r, iCodprod);
            String codvolPadrao  = readText(r, iCodvolPad);
            String codvolItem    = readText(r, iCodvolItem);
            String controle      = readText(r, iControle);
            String qtdnegStr     = readText(r, iQtdneg);

            Integer codProdInt = null;
            if (codprodStr != null && !codprodStr.isBlank()) {
                try {
                    codProdInt = Integer.valueOf(codprodStr);
                } catch (NumberFormatException e) {
                    log.warn("CODPROD_INVALID nunota={} seq={} codprod='{}'",
                            nunotaOrig, seqInt, codprodStr);
                }
            }

            Double qtdOriginal = null;
            if (qtdnegStr != null && !qtdnegStr.isBlank()) {
                try {
                    qtdOriginal = Double.valueOf(qtdnegStr);
                } catch (NumberFormatException e) {
                    log.warn("QTDNEG_INVALID nunota={} seq={} qtd='{}'",
                            nunotaOrig, seqInt, qtdnegStr);
                }
            }

            // snapshot original no Mongo (por SEQUENCIA)
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
                    log.warn("MONGO_SNAPSHOT_FALHA nunota={} seq={}", nunotaOrig, seqInt, e);
                }
            }

            if (codprodStr != null && !codprodStr.isBlank() && qtdOriginal != null) {
                double qtdEmPadrao = converterParaUnidadePadrao(codvolItem, codvolPadrao, qtdOriginal);
                String chave = codprodStr + "|" + (controle == null ? "" : controle);

                ItemAgrupadoPorProduto agg = agrupados.get(chave);
                if (agg == null) {
                    agg = new ItemAgrupadoPorProduto(
                            codprodStr,
                            codvolPadrao,
                            controle,
                            qtdEmPadrao
                    );
                    agrupados.put(chave, agg);
                } else {
                    agg.somar(qtdEmPadrao);
                }
            }

            count++;
        }

        ObjectNode rootReq = objectMapper.createObjectNode();
        ObjectNode requestBody2 = rootReq.putObject("requestBody");
        ObjectNode dataSet = requestBody2.putObject("dataSet");

        dataSet.put("rootEntity", "DetalhesConferencia");
        dataSet.put("includePresentationFields", "S");

        ArrayNode dataRowArray = dataSet.putArray("dataRow");

        int seqConfGlobal = 1;
        for (ItemAgrupadoPorProduto agg : agrupados.values()) {
            ObjectNode rowNode = dataRowArray.addObject();
            ObjectNode local = rowNode.putObject("localFields");

            String seqConfStr = String.valueOf(seqConfGlobal++);
            String qtdSomadaStr = String.format(Locale.US, "%.4f", agg.qtdTotalPadrao);

            local.putObject("NUCONF").put("$", nuconf.toString());
            local.putObject("SEQCONF").put("$", seqConfStr);
            local.putObject("CODBARRA").put("$", "");
            local.putObject("CODPROD").put("$", agg.codProdStr);
            local.putObject("CODVOL").put("$", agg.codvolPadrao == null ? "" : agg.codvolPadrao);
            local.putObject("CONTROLE").put("$", agg.controle == null ? "" : agg.controle);
            local.putObject("QTDCONFVOLPAD").put("$", qtdSomadaStr);
            local.putObject("QTDCONF").put("$", qtdSomadaStr);
            local.putObject("COPIA").put("$", "N");
        }

        ObjectNode entity = dataSet.putObject("entity");
        ObjectNode fieldSet = entity.putObject("fieldSet");
        fieldSet.put("list",
                "NUCONF,SEQCONF,CODBARRA,CODPROD,CODVOL,CONTROLE,QTDCONFVOLPAD,QTDCONF,COPIA"
        );

        log.info("PREENCHER_ITENS_SAVE nunotaOrig={} nuconf={} itens={}",
                nunotaOrig, nuconf, dataRowArray.size());

        JsonNode resp = gatewayClient.callService("CRUDServiceProvider.saveRecord", rootReq);
        log.info("PREENCHER_ITENS_FIM nunotaOrig={} nuconf={} respNula={}",
                nunotaOrig, nuconf, resp == null);
        return resp;
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

        return gatewayClient.callService("CRUDServiceProvider.saveRecord", root);
    }

    // ---------------------------------
    // FLUXO OK COMPLETO:
    // 1) FINALIZA CABEÇALHO (STATUS = F)
    // 2) BUSCA NUNOTAORIG EM TGFCON2
    // 3) COPIA ITENS (TGFITE -> TGFCOI2) + REGISTRA NO MONGO
    // ---------------------------------
    public JsonNode finalizarConferenciaOkComItens(Long nuconf, Long codUsuario) {
        log.info("FINALIZAR_OK_COM_ITENS nuconf={} codUsuario={}", nuconf, codUsuario);

        JsonNode respCabecalho = finalizarConferencia(nuconf, codUsuario);

        Long nunotaOrig = buscarNunotaOrigPorNuconf(nuconf);
        if (nunotaOrig == null) {
            log.warn("FINALIZAR_OK_SEM_NUNOTAORIG nuconf={}", nuconf);

            ObjectNode combinado = objectMapper.createObjectNode();
            combinado.set("cabecalhoConferencia", respCabecalho);
            combinado.put("nunotaOrig", (String) null);
            combinado.put("nuconf", nuconf);
            combinado.put("statusItens", "NAO_PREENCHIDOS_NUNOTAORIG_NULO");
            return combinado;
        }

        JsonNode respDetalhes = preencherItensConferencia(nunotaOrig, nuconf);

        ObjectNode combinado = objectMapper.createObjectNode();
        combinado.set("detalhesConferencia", respDetalhes);
        combinado.set("cabecalhoConferencia", respCabecalho);
        combinado.put("nunotaOrig", nunotaOrig);
        combinado.put("nuconf", nuconf);
        return combinado;
    }

    // ---------------------------------
    // FINALIZAR CONFERÊNCIA DIVERGENTE
    // (apenas registra no app, não mexe no Sankhya)
    // ---------------------------------
    public JsonNode finalizarConferenciaDivergente(FinalizarDivergenteRequest req) {
        Long nuconf = req.nuconf();
        Long nunotaOrig = req.nunotaOrig();

        try {
            log.info("FINALIZAR_DIVERGENTE_REQ nuconf={} nunotaOrig={} itens={}",
                    nuconf, nunotaOrig,
                    req.itens() != null ? req.itens().size() : 0);
        } catch (Exception ignored) {
        }

        ObjectNode resp = objectMapper.createObjectNode();
        resp.put("status", "OK");
        resp.put("mensagem",
                "Conferência divergente registrada apenas no app. " +
                        "Nenhum item foi gerado/ajustado no Sankhya.");
        resp.put("nunotaOrig", nunotaOrig);
        resp.put("nuconf", nuconf);
        return resp;
    }

    // ----------------- MÉTODOS ANTIGOS (AUXILIARES / OPCIONAIS) -----------------

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

            localFields.putObject("QTDCONF").put("$", item.qtdConferida().toString());

            ObjectNode key = row.putObject("key");
            key.putObject("NUCONF").put("$", nuconf.toString());
            key.putObject("SEQCONF").put("$", item.sequencia().toString());
        }

        ObjectNode entity = dataSet.putObject("entity");
        ObjectNode fieldSet = entity.putObject("fieldSet");
        fieldSet.put("list", "NUCONF,SEQCONF,QTDCONF");

        JsonNode response = gatewayClient.callService("CRUDServiceProvider.saveRecord", root);
        log.info("TGFCOI2_ATUALIZADO nuconf={} sucesso={}", nuconf, response != null);
    }

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
            if (item.qtdConferida() == null) continue;

            Double qtdConf = item.qtdConferida();
            double vlrUnit = buscarVlrUnitItem(nunotaOrig, item.sequencia(), item.codProd());
            double novoVlrTot = vlrUnit * qtdConf;

            String qtdConfStr = String.format(Locale.US, "%.4f", qtdConf);
            String vlrTotStr = String.format(Locale.US, "%.2f", novoVlrTot);

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

        JsonNode response = gatewayClient.callService("CRUDServiceProvider.saveRecord", root);
        log.info("TGFITE_ATUALIZADO nunota={} sucesso={}", nunotaOrig, response != null);
    }

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

        return gatewayClient.callService("CRUDServiceProvider.saveRecord", root);
    }

    // ----------------- API para o PedidoConferenciaService -----------------
    public Double getQtdOriginalItem(Long nunota, Integer sequencia, Double fallbackQtdAtual) {
        log.info("QTD_ORIGINAL_GET nunota={} seq={}", nunota, sequencia);

        if (nunota == null || sequencia == null) {
            return fallbackQtdAtual;
        }

        return conferenciaItemMongoService
                .buscar(nunota, sequencia)
                .map(ItemConferenciaDoc::getQtdOriginal)
                .orElse(fallbackQtdAtual);
    }

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
            log.warn("VLRUNIT_NAO_ENCONTRADO nunota={} seq={} codProd={}",
                    nunotaOrig, sequencia, codProd);
            return 0.0;
        }

        List<String> cols = extractColumns(fieldsMetadata);
        int idxVlrUnit = indexOf(cols, "VLRUNIT");
        if (idxVlrUnit < 0) {
            log.warn("COL_VLRUNIT_NAO_ENCONTRADA nunota={} seq={} codProd={}",
                    nunotaOrig, sequencia, codProd);
            return 0.0;
        }

        String vlrUnitStr = readText(rows.get(0), idxVlrUnit);
        if (vlrUnitStr == null || vlrUnitStr.isBlank()) return 0.0;

        try {
            return Double.parseDouble(vlrUnitStr);
        } catch (NumberFormatException e) {
            log.error("VLRUNIT_CONVERSAO_ERRO nunota={} seq={} codProd={} valor='{}'",
                    nunotaOrig, sequencia, codProd, vlrUnitStr, e);
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
