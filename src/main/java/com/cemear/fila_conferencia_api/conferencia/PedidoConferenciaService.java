package com.cemear.fila_conferencia_api.conferencia;

import com.cemear.fila_conferencia_api.sankhya.SankhyaGatewayClient;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class PedidoConferenciaService {

    private final SankhyaGatewayClient gatewayClient;

    // Template com PLACEHOLDERS para filtros + paginação
    private static final String SQL_PEDIDOS_PAGINADO_TEMPLATE = """
    SELECT *
    FROM (
        SELECT
            X.NUNOTA,
            CAB.NUMNOTA,
            PAR.NOMEPARC,
            X.STATUS_CONFERENCIA,
            ITE.SEQUENCIA,
            ITE.CODPROD,
            PRO.DESCRPROD AS DESCRICAO,
            ITE.CODVOL                       AS UNIDADE,
            ITE.QTDNEG,
            ITE.VLRUNIT,
            ITE.VLRTOT,
            ROW_NUMBER() OVER (
                ORDER BY X.NUNOTA DESC, ITE.SEQUENCIA
            ) AS RN
        FROM (
            SELECT
                CAB.NUNOTA,
                CAB.CODPARC,
                SANKHYA.SNK_GET_SATUSCONFERENCIA(CAB.NUNOTA) AS STATUS_CONFERENCIA
            FROM TGFCAB CAB
            WHERE 1 = 1
              %s   -- Filtro de data
        ) X
        JOIN TGFCAB CAB
            ON CAB.NUNOTA = X.NUNOTA
        JOIN TGFPAR PAR
            ON PAR.CODPARC = CAB.CODPARC
        JOIN TGFITE ITE
            ON ITE.NUNOTA = X.NUNOTA
        JOIN TGFPRO PRO
            ON PRO.CODPROD = ITE.CODPROD
        WHERE X.STATUS_CONFERENCIA IN (
            'A', 'AC', 'AL', 'C',
            'D', 'F',
            'R', 'RA', 'RD', 'RF',
            'Z'
        )
          %s   -- Filtro de status
    )
    WHERE RN BETWEEN %d AND %d
    ORDER BY RN
    """;


    // opcional: método default sem filtros (se usar em outro lugar)
    public List<PedidoConferenciaDto> listarPendentes() {
        return listarPendentesPaginado(0, 50, null, null, null);
    }

    // paginação + filtros
    public List<PedidoConferenciaDto> listarPendentesPaginado(
            int page,
            int pageSize,
            String status,
            LocalDate dataIni,
            LocalDate dataFim
    ) {

        int inicio = page * pageSize + 1;          // RN começa em 1
        int fim = inicio + pageSize - 1;

        // ---------- Filtro de data ----------
        String filtroDataSql;
        if (dataIni != null && dataFim != null) {
            DateTimeFormatter fmt = DateTimeFormatter.ofPattern("dd/MM/yyyy");

            String iniStr = dataIni.format(fmt);
            String fimStr = dataFim.format(fmt);

            filtroDataSql = String.format("""
                    AND CAB.DTNEG >= TO_DATE('%s', 'DD/MM/YYYY')
                    AND CAB.DTNEG <  TO_DATE('%s', 'DD/MM/YYYY') + 1
                    """, iniStr, fimStr);
        } else {
            // default: mês atual
            filtroDataSql = """
                    AND CAB.DTNEG >= TRUNC(SYSDATE, 'MM')
                    AND CAB.DTNEG <  ADD_MONTHS(TRUNC(SYSDATE, 'MM'), 1)
                    """;
        }

        // ---------- Filtro de status ----------
        String filtroStatusSql = "";
        if (status != null && !status.isBlank()) {
            filtroStatusSql = " AND X.STATUS_CONFERENCIA = '" + status.trim() + "'";
        }

        // monta SQL final (4 argumentos: %s, %s, %d, %d)
        String sql = String.format(
                SQL_PEDIDOS_PAGINADO_TEMPLATE,
                filtroDataSql,
                filtroStatusSql,
                inicio,
                fim
        );

        JsonNode root = gatewayClient.executeDbExplorer(sql);

        JsonNode responseBody = root.path("responseBody");
        JsonNode fieldsMetadata = responseBody.path("fieldsMetadata");
        JsonNode rowsNode = responseBody.path("rows");

        if (!rowsNode.isArray()) {
            log.error("Resposta inesperada do DbExplorer: {}", root.toPrettyString());
            throw new IllegalStateException("DbExplorer não retornou array 'rows' na resposta.");
        }

        ArrayNode rows = (ArrayNode) rowsNode;

        List<String> cols = extractColumns(fieldsMetadata);

        int iNunota = indexOf(cols, "NUNOTA");
        int iNumNota = indexOf(cols, "NUMNOTA");     // 👈 número da nota (TGFCAB.NUMNOTA)
        int iNomeParc = indexOf(cols, "NOMEPARC");    // 👈 nome do parceiro/cliente
        int iStatus = indexOf(cols, "STATUS_CONFERENCIA");
        int iSeq = indexOf(cols, "SEQUENCIA");
        int iCodProd = indexOf(cols, "CODPROD");
        int iDescricao = indexOf(cols, "DESCRICAO");   // descrição do produto
        int iUnidade = indexOf(cols, "UNIDADE");     // CODVOL do item
        int iQtdNeg = indexOf(cols, "QTDNEG");
        int iVlrUnit = indexOf(cols, "VLRUNIT");
        int iVlrTot = indexOf(cols, "VLRTOT");


        if (iNunota < 0 || iStatus < 0) {
            log.error("Campos obrigatórios não encontrados em fieldsMetadata: {}", cols);
            throw new IllegalStateException("Campos NUNOTA/STATUS_CONFERENCIA não encontrados na resposta do DbExplorer.");
        }

        // 🔥 AGRUPAR POR NUNOTA
        LinkedHashMap<Long, PedidoConferenciaDto> pedidosMap = new LinkedHashMap<>();

        for (JsonNode r : rows) {
            if (!r.isArray()) continue;

            Long nunota = readLong(r, iNunota);
            Long numNota = (iNumNota >= 0) ? readLong(r, iNumNota) : null;
            String nomeParc = (iNomeParc >= 0) ? readText(r, iNomeParc) : null;
            String st = readText(r, iStatus);
            Integer sequencia = readInt(r, iSeq);
            Long codProd = readLong(r, iCodProd);
            String descricao = readText(r, iDescricao);
            String unidade = readText(r, iUnidade);   // ex: MT, PC, CX
            Double qtdNeg = readDouble(r, iQtdNeg);
            Double vlrUnit = readDouble(r, iVlrUnit);
            Double vlrTot = readDouble(r, iVlrTot);

            if (nunota == null || st == null) {
                continue;
            }

            // pega (ou cria) o pedido
            PedidoConferenciaDto pedido = pedidosMap.get(nunota);
            if (pedido == null) {
                // 👇 agora passando numNota e nomeParc pro DTO novo
                pedido = new PedidoConferenciaDto(nunota, numNota, nomeParc, st);
                pedidosMap.put(nunota, pedido);
            } else {
                // se já existia, garante que numNota/nomeParc estão preenchidos
                if (pedido.getNumNota() == null) {
                    pedido.setNumNota(numNota);
                }
                if (pedido.getNomeParc() == null) {
                    pedido.setNomeParc(nomeParc);
                }
            }

            // adiciona o item
            pedido.addItem(new ItemConferenciaDto(
                    sequencia,
                    codProd,
                    descricao,
                    unidade,
                    qtdNeg,
                    vlrUnit,
                    vlrTot
            ));
        }

        return new ArrayList<>(pedidosMap.values());
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
