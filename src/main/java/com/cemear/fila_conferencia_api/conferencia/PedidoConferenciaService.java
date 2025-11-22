package com.cemear.fila_conferencia_api.conferencia;

import com.cemear.fila_conferencia_api.sankhya.SankhyaGatewayClient;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class PedidoConferenciaService {

    private final SankhyaGatewayClient gatewayClient;

    // Template com PLACEHOLDERS para início/fim da página
    private static final String SQL_PEDIDOS_PAGINADO_TEMPLATE = """
        SELECT *
        FROM (
            SELECT
                X.NUNOTA,
                X.STATUS_CONFERENCIA,
                ITE.SEQUENCIA,
                ITE.CODPROD,
                ITE.QTDNEG,
                ITE.VLRUNIT,
                ITE.VLRTOT,
                ROW_NUMBER() OVER (ORDER BY X.NUNOTA, ITE.SEQUENCIA) AS RN
            FROM (
                SELECT
                    CAB.NUNOTA,
                    SANKHYA.SNK_GET_SATUSCONFERENCIA(CAB.NUNOTA) AS STATUS_CONFERENCIA
                FROM TGFCAB CAB
                WHERE CAB.DTNEG >= TRUNC(SYSDATE, 'MM')              -- 1º dia do mês atual
                  AND CAB.DTNEG <  ADD_MONTHS(TRUNC(SYSDATE, 'MM'), 1) -- 1º dia do próximo mês
            ) X
            JOIN TGFITE ITE
                ON ITE.NUNOTA = X.NUNOTA
            WHERE X.STATUS_CONFERENCIA IN (
                'A', 'AC', 'AL', 'C',
                'D', 'F',
                'R', 'RA', 'RD', 'RF',
                'Z'
            )
        )
        WHERE RN BETWEEN %d AND %d
        ORDER BY RN
        """;

    // método antigo, se quiser manter um default
    public List<PedidoConferenciaDto> listarPendentes() {
        return listarPendentesPaginado(0, 50); // página 0, 50 itens por página
    }

    // 🔹 Novo: paginação
    public List<PedidoConferenciaDto> listarPendentesPaginado(int page, int pageSize) {

        int inicio = page * pageSize + 1;          // RN começa em 1
        int fim    = inicio + pageSize - 1;

        String sql = String.format(SQL_PEDIDOS_PAGINADO_TEMPLATE, inicio, fim);

        JsonNode root = gatewayClient.executeDbExplorer(sql);

        JsonNode responseBody   = root.path("responseBody");
        JsonNode fieldsMetadata = responseBody.path("fieldsMetadata");
        JsonNode rowsNode       = responseBody.path("rows");

        if (!rowsNode.isArray()) {
            log.error("Resposta inesperada do DbExplorer: {}", root.toPrettyString());
            throw new IllegalStateException("DbExplorer não retornou array 'rows' na resposta.");
        }

        ArrayNode rows = (ArrayNode) rowsNode;

        List<String> cols = extractColumns(fieldsMetadata);

        int iNunota   = indexOf(cols, "NUNOTA");
        int iStatus   = indexOf(cols, "STATUS_CONFERENCIA");
        int iSeq      = indexOf(cols, "SEQUENCIA");
        int iCodProd  = indexOf(cols, "CODPROD");
        int iQtdNeg   = indexOf(cols, "QTDNEG");
        int iVlrUnit  = indexOf(cols, "VLRUNIT");
        int iVlrTot   = indexOf(cols, "VLRTOT");

        if (iNunota < 0 || iStatus < 0) {
            log.error("Campos obrigatórios não encontrados em fieldsMetadata: {}", cols);
            throw new IllegalStateException("Campos NUNOTA/STATUS_CONFERENCIA não encontrados na resposta do DbExplorer.");
        }

        List<PedidoConferenciaDto> result = new ArrayList<>();

        for (JsonNode r : rows) {
            if (!r.isArray()) continue;

            Long nunota       = readLong(r, iNunota);
            String status     = readText(r, iStatus);
            Integer sequencia = readInt(r, iSeq);
            Long codProd      = readLong(r, iCodProd);
            Double qtdNeg     = readDouble(r, iQtdNeg);
            Double vlrUnit    = readDouble(r, iVlrUnit);
            Double vlrTot     = readDouble(r, iVlrTot);

            if (nunota != null && status != null) {
                result.add(new PedidoConferenciaDto(
                        nunota,
                        status,
                        sequencia,
                        codProd,
                        qtdNeg,
                        vlrUnit,
                        vlrTot
                ));
            }
        }

        return result;
    }

    // ---------- helpers ----------

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
        return (v == null || v.isNull()) ? null : v.asText(null);
    }

    private static Long readLong(JsonNode row, int idx) {
        String s = readText(row, idx);
        if (s == null || s.isBlank()) return null;
        try {
            return Long.valueOf(s);
        } catch (NumberFormatException e) {
            return row.get(idx).asLong();
        }
    }

    private static Integer readInt(JsonNode row, int idx) {
        String s = readText(row, idx);
        if (s == null || s.isBlank()) return null;
        try {
            return Integer.valueOf(s);
        } catch (NumberFormatException e) {
            return row.get(idx).asInt();
        }
    }

    private static Double readDouble(JsonNode row, int idx) {
        String s = readText(row, idx);
        if (s == null || s.isBlank()) return null;
        try {
            return Double.valueOf(s);
        } catch (NumberFormatException e) {
            return row.get(idx).asDouble();
        }
    }
}
