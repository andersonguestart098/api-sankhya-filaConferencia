package com.cemear.fila_conferencia_api.conferencia;

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
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class ConferenciaWorkflowService {

    private final SankhyaGatewayClient gatewayClient;
    private final ObjectMapper objectMapper;

    private static final DateTimeFormatter FMT =
            DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm");

    // ---------------------------------
    // JÁ EXISTENTE: iniciar conferência
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


    // ===================== CABECALHO NOTA: NUCONFATUAL =====================
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




    // ------------------------------------------------------
// NOVO: copiar itens da TGFITE para TGFCOI2 (DetalhesConferencia)
// ------------------------------------------------------
    public JsonNode preencherItensConferencia(Long nunotaOrig, Long nuconf) {

        // 1) Busca itens do pedido na TGFITE via DbExplorer
        String sql = """
            SELECT
                i.SEQUENCIA,
                i.CODPROD,
                i.CODVOL,
                i.CONTROLE,
                i.QTDNEG
            FROM TGFITE i
            WHERE i.NUNOTA = %d
            """.formatted(nunotaOrig);

        // volta o JSON COMPLETO do Sankhya (status + responseBody)
        JsonNode root = gatewayClient.executeDbExplorer(sql);

        // desce até o responseBody
        JsonNode responseBody = root.path("responseBody");
        JsonNode fieldsMetadata = responseBody.path("fieldsMetadata");
        JsonNode rowsNode = responseBody.path("rows");

        // se não tiver rows como array, loga e para
        if (!rowsNode.isArray()) {
            log.error("Resposta inesperada do DbExplorer ao preencher itens conferência: {}",
                    root.toPrettyString());
            throw new IllegalStateException("DbExplorer não retornou array 'rows' na resposta.");
        }

        ArrayNode rows = (ArrayNode) rowsNode;

        // mapeia colunas pelos metadados
        List<String> cols = extractColumns(fieldsMetadata);

        int iSequencia = indexOf(cols, "SEQUENCIA");
        int iCodprod   = indexOf(cols, "CODPROD");
        int iCodvol    = indexOf(cols, "CODVOL");
        int iControle  = indexOf(cols, "CONTROLE");
        int iQtdneg    = indexOf(cols, "QTDNEG");

        // 2) Monta o body do DetalhesConferencia com dataRow[]
        ObjectNode rootReq = objectMapper.createObjectNode();
        ObjectNode requestBody = rootReq.putObject("requestBody");
        ObjectNode dataSet = requestBody.putObject("dataSet");

        dataSet.put("rootEntity", "DetalhesConferencia");
        dataSet.put("includePresentationFields", "S");

        ArrayNode dataRowArray = dataSet.putArray("dataRow");

        int count = 0;
        for (JsonNode r : rows) {
            if (!r.isArray()) continue;

            String seqConf = readText(r, iSequencia);
            if (seqConf == null || seqConf.isBlank()) {
                seqConf = String.valueOf(count + 1);
            }

            String codprod  = readText(r, iCodprod);
            String codvol   = readText(r, iCodvol);
            String controle = readText(r, iControle);
            String qtdneg   = readText(r, iQtdneg); // já vem no formato que o Sankhya espera

            ObjectNode rowNode = dataRowArray.addObject();
            ObjectNode local = rowNode.putObject("localFields");

            local.putObject("NUCONF").put("$", nuconf.toString());
            local.putObject("SEQCONF").put("$", seqConf);
            local.putObject("CODBARRA").put("$", "");
            local.putObject("CODPROD").put("$", codprod);
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

        // 3) Chama o Gateway
        return gatewayClient.callService(
                "CRUDServiceProvider.saveRecord",
                rootReq
        );
    }


    // ---------------------------------
    // JÁ DESENHADO: finalizar conferência
    // (STATUS = F, ajusta DHFINCONF, etc.)
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

    // ----------------- helpers reaproveitados -----------------

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
