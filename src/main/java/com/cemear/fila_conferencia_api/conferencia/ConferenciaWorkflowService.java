package com.cemear.fila_conferencia_api.conferencia;

import com.cemear.fila_conferencia_api.auth.dto.FinalizarDivergenteRequest;
import com.cemear.fila_conferencia_api.auth.dto.FinalizarDivergenteRequest.ItemFinalizacaoDTO;
import com.cemear.fila_conferencia_api.conferencia.mongo.ConferenciaItemMongoService;
import com.cemear.fila_conferencia_api.conferencia.mongo.ItemConferenciaDoc;
import com.cemear.fila_conferencia_api.conferencia.mongo.PedidoConferenciaMongoService;
import com.cemear.fila_conferencia_api.sankhya.SankhyaGatewayClient;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class ConferenciaWorkflowService {

    private final SankhyaGatewayClient gatewayClient;
    private final ObjectMapper objectMapper;
    private final ConferenciaItemMongoService conferenciaItemMongoService;
    private final PedidoConferenciaMongoService pedidoConferenciaMongoService;

    private static final DateTimeFormatter FMT =
            DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm");

    // Estrutura interna para agrupar itens por CODPROD
    private static class ItemAgrupado {
        final String codProdStr;
        final String codvol;
        final String controle;
        double qtdTotal;

        ItemAgrupado(String codProdStr, String codvol, String controle, double qtdInicial) {
            this.codProdStr = codProdStr;
            this.codvol = codvol;
            this.controle = controle;
            this.qtdTotal = qtdInicial;
        }

        void somar(double qtd) {
            this.qtdTotal += qtd;
        }
    }

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
            return false;
        }

        List<String> cols = extractColumns(fieldsMetadata);
        int idxStatus = indexOf(cols, "STATUS");
        if (idxStatus < 0) {
            return false;
        }

        String status = readText(rows.get(0), idxStatus);
        return "F".equalsIgnoreCase(status);
    }

    // ---------------------------------
    // Buscar NUNOTAORIG a partir do NUCONF em TGFCON2
    // ---------------------------------
    private Long buscarNunotaOrigPorNuconf(Long nuconf) {
        if (nuconf == null) {
            return null;
        }

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
            return null;
        }

        List<String> cols = extractColumns(fieldsMetadata);
        int idxNunotaOrig = indexOf(cols, "NUNOTAORIG");
        if (idxNunotaOrig < 0) {
            return null;
        }

        String nunotaStr = readText(rows.get(0), idxNunotaOrig);
        if (nunotaStr == null || nunotaStr.isBlank()) {
            return null;
        }

        try {
            return Long.valueOf(nunotaStr);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    // ------------------------------------------------------
    // COPIAR ITENS DA TGFITE PARA TGFCOI2 (DetalhesConferencia)
    // + REGISTRAR QTDNEG ORIGINAL NO MONGO
    //
    // NOVO COMPORTAMENTO:
    // - AGRUPA ITENS POR CODPROD
    // - SOMA QTDNEG DOS ITENS COM MESMO CÓDIGO
    // - INSERE APENAS UMA LINHA POR PRODUTO EM TGFCOI2
    //
    // ESTE MÉTODO SÓ AGE SE STATUS = 'F' EM TGFCON2.
    // ------------------------------------------------------
    public JsonNode preencherItensConferencia(Long nunotaOrig, Long nuconf) {

        // 🔒 Segurança: só preenche itens se NUCONF estiver com STATUS = 'F'
        if (!conferenciaEstaFinalizadaOk(nuconf)) {
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

        JsonNode root = gatewayClient.executeDbExplorer(sql);
        JsonNode responseBody = root.path("responseBody");
        JsonNode fieldsMetadata = responseBody.path("fieldsMetadata");
        JsonNode rowsNode = responseBody.path("rows");

        if (!rowsNode.isArray()) {
            throw new IllegalStateException("DbExplorer não retornou array 'rows' na resposta.");
        }

        ArrayNode rows = (ArrayNode) rowsNode;
        List<String> cols = extractColumns(fieldsMetadata);

        int iSequencia = indexOf(cols, "SEQUENCIA");
        int iCodprod   = indexOf(cols, "CODPROD");
        int iCodvol    = indexOf(cols, "CODVOL");
        int iControle  = indexOf(cols, "CONTROLE");
        int iQtdneg    = indexOf(cols, "QTDNEG");

        // Mapa para agrupar por CODPROD e somar quantidades
        Map<String, ItemAgrupado> agrupadosPorProduto = new LinkedHashMap<>();

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
                    // mantém comportamento: ignora conversão
                }
            }

            Double qtdOriginal = null;
            if (qtdneg != null && !qtdneg.isBlank()) {
                try {
                    qtdOriginal = Double.valueOf(qtdneg);
                } catch (NumberFormatException e) {
                    // mantém comportamento: ignora conversão
                }
            }

            // REGISTRA SNAPSHOT ORIGINAL NO MONGO (por SEQUENCIA, como já fazia antes)
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
                    // mantém comportamento: ignora falha no mongo
                }
            }

            // AGRUPA POR CODPROD PARA POPULAR A TGFCOI2
            if (codprodStr != null && !codprodStr.isBlank() && qtdOriginal != null) {
                ItemAgrupado agg = agrupadosPorProduto.get(codprodStr);
                if (agg == null) {
                    // guarda o primeiro CODVOL/CONTROLE encontrados para este produto
                    agg = new ItemAgrupado(
                            codprodStr,
                            codvol,
                            controle == null ? "" : controle,
                            qtdOriginal
                    );
                    agrupadosPorProduto.put(codprodStr, agg);
                } else {
                    agg.somar(qtdOriginal);
                }
            }

            count++;
        }

        // Monta payload para DetalhesConferencia (TGFCOI2) com itens AGRUPADOS
        ObjectNode rootReq = objectMapper.createObjectNode();
        ObjectNode requestBody2 = rootReq.putObject("requestBody");
        ObjectNode dataSet = requestBody2.putObject("dataSet");

        dataSet.put("rootEntity", "DetalhesConferencia");
        dataSet.put("includePresentationFields", "S");

        ArrayNode dataRowArray = dataSet.putArray("dataRow");

        int seqConfGlobal = 1;
        for (ItemAgrupado agg : agrupadosPorProduto.values()) {
            ObjectNode rowNode = dataRowArray.addObject();
            ObjectNode local = rowNode.putObject("localFields");

            String seqConfStr = String.valueOf(seqConfGlobal++);
            String qtdSomadaStr = String.format(Locale.US, "%.4f", agg.qtdTotal);

            local.putObject("NUCONF").put("$", nuconf.toString());
            local.putObject("SEQCONF").put("$", seqConfStr);
            local.putObject("CODBARRA").put("$", "");
            local.putObject("CODPROD").put("$", agg.codProdStr);
            local.putObject("CODVOL").put("$", agg.codvol);
            local.putObject("CONTROLE").put("$", agg.controle != null ? agg.controle : "");
            local.putObject("QTDCONFVOLPAD").put("$", qtdSomadaStr);
            local.putObject("QTDCONF").put("$", qtdSomadaStr);
            local.putObject("COPIA").put("$", "N");
        }

        ObjectNode entity = dataSet.putObject("entity");
        ObjectNode fieldSet = entity.putObject("fieldSet");
        fieldSet.put("list",
                "NUCONF,SEQCONF,CODBARRA,CODPROD,CODVOL,CONTROLE,QTDCONFVOLPAD,QTDCONF,COPIA"
        );

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
    // FLUXO OK COMPLETO:
    // 1) FINALIZA CABEÇALHO (STATUS = F)
    // 2) BUSCA NUNOTAORIG EM TGFCON2
    // 3) COPIA ITENS (TGFITE -> TGFCOI2) + REGISTRA NO MONGO
    // ---------------------------------
    public JsonNode finalizarConferenciaOkComItens(Long nuconf, Long codUsuario) {

        // 1) Finaliza cabeçalho (STATUS = F)
        JsonNode respCabecalho = finalizarConferencia(nuconf, codUsuario);

        // 2) Buscar NUNOTAORIG na TGFCON2
        Long nunotaOrig = buscarNunotaOrigPorNuconf(nuconf);
        if (nunotaOrig == null) {
            ObjectNode combinado = objectMapper.createObjectNode();
            combinado.set("cabecalhoConferencia", respCabecalho);
            combinado.put("nunotaOrig", (String) null);
            combinado.put("nuconf", nuconf);
            combinado.put("statusItens", "NAO_PREENCHIDOS_NUNOTAORIG_NULO");
            return combinado;
        }

        // 3) Preenche itens de conferência (agora STATUS já é F)
        JsonNode respDetalhes = preencherItensConferencia(nunotaOrig, nuconf);

        // 4) Monta uma resposta combinada (opcional)
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
    // - NÃO ATUALIZA TGFCOI2
    // - NÃO ATUALIZA TGFITE
    // - NÃO MUDA STATUS (CABEÇALHO FICA EM "A")
    // - NÃO SALVA NEM NO MONGO
    // ---------------------------------
    public JsonNode finalizarConferenciaDivergente(FinalizarDivergenteRequest req) {
        Long nuconf = req.nuconf();
        Long nunotaOrig = req.nunotaOrig();
        Long codUsuario = req.codUsuario();
        List<ItemFinalizacaoDTO> itens = req.itens();

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

        gatewayClient.callService("CRUDServiceProvider.saveRecord", root);
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
                continue;
            }

            Double qtdConf = item.qtdConferida();

            // 1) Buscar VLRUNIT do item na TGFITE
            double vlrUnit = buscarVlrUnitItem(nunotaOrig, item.sequencia(), item.codProd());

            // 2) Calcular novo VLRTOT
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

        gatewayClient.callService("CRUDServiceProvider.saveRecord", root);
    }

    // finaliza cabeçalho com STATUS = D (não usado no novo fluxo)
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

        return gatewayClient.callService(
                "CRUDServiceProvider.saveRecord",
                root
        );
    }

    // ----------------- API para o PedidoConferenciaService -----------------
    public Double getQtdOriginalItem(Long nunota, Integer sequencia, Double fallbackQtdAtual) {

        if (nunota == null || sequencia == null) {
            return fallbackQtdAtual;
        }

        return conferenciaItemMongoService
                .buscar(nunota, sequencia)
                .map(ItemConferenciaDoc::getQtdOriginal)
                .orElse(fallbackQtdAtual);
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
            return 0.0;
        }

        List<String> cols = extractColumns(fieldsMetadata);
        int idxVlrUnit = indexOf(cols, "VLRUNIT");
        if (idxVlrUnit < 0) {
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
            return 0.0;
        }
    }

    // ============================================================
    // DEFINIR CONFERENTE (persistência no Mongo)
    // ============================================================
    public void definirConferente(Long nunota, Integer conferenteId, String conferenteNome) {

        if (nunota == null) throw new IllegalArgumentException("nunota é obrigatório");
        if (conferenteId == null) throw new IllegalArgumentException("conferenteId é obrigatório");
        if (conferenteNome == null || conferenteNome.trim().isEmpty()) {
            throw new IllegalArgumentException("conferenteNome é obrigatório");
        }

        pedidoConferenciaMongoService.definirConferente(
                nunota,
                conferenteId,
                conferenteNome.trim()
        );
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
