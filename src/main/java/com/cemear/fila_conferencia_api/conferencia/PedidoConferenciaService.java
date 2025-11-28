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
    private final ConferenciaWorkflowService conferenciaWorkflowService; // 👈 para ler qtdOriginal

    // Template com PLACEHOLDERS para filtros + paginação
    // Inclui CODVEND + NOME_VENDEDOR e campos de conferência
    private static final String SQL_PEDIDOS_PAGINADO_TEMPLATE = """
    SELECT *
    FROM (
        SELECT
            X.NUNOTA,
            CAB.NUMNOTA,
            PAR.NOMEPARC,
            X.STATUS_CONFERENCIA,
            CAB.CODVEND,
            VEND.APELIDO AS NOME_VENDEDOR,

            ITE.SEQUENCIA,
            ITE.CODPROD,
            PRO.DESCRPROD AS DESCRICAO,
            ITE.CODVOL                       AS UNIDADE,

            -- quantidade atual da nota (TGFITE já “cortada”)
            ITE.QTDNEG                       AS QTD_ATUAL,

            -- quantidades da conferência (TGFCOI2)
            COI.QTDCONFVOLPAD                AS QTD_ESPERADA,
            COI.QTDCONF                      AS QTD_CONFERIDA,

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
        LEFT JOIN TGFVEN VEND
            ON VEND.CODVEND = CAB.CODVEND

        -- junta com os itens da conferência (TGFCOI2)
        LEFT JOIN TGFCOI2 COI
            ON COI.NUCONF  = CAB.NUCONFATUAL   -- nuconf atual da nota
           AND COI.SEQCONF = ITE.SEQUENCIA     -- mesmo item

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

    // método default sem filtros
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

        // monta SQL final
        String sql = String.format(
                SQL_PEDIDOS_PAGINADO_TEMPLATE,
                filtroDataSql,
                filtroStatusSql,
                inicio,
                fim
        );

        // 🔥 LOG ESCANDALOSO DO SQL
        log.info("\n\n================= DEBUG_PEDIDOS_CONF =================");
        log.info("page={}, pageSize={}, status={}, dataIni={}, dataFim={}",
                page, pageSize, status, dataIni, dataFim);
        log.info("SQL GERADO:\n{}\n======================================================\n", sql);

        JsonNode root = gatewayClient.executeDbExplorer(sql);

        // 🔥 LOG ESCANDALOSO DA RESPOSTA BRUTA
        log.info("########## DEBUG_PEDIDOS_CONF - RESPOSTA DBEXPLORER ##########");
        log.info("Resposta completa do DbExplorer:\n{}", root.toPrettyString());
        log.info("################################################################");

        JsonNode responseBody = root.path("responseBody");
        JsonNode fieldsMetadata = responseBody.path("fieldsMetadata");
        JsonNode rowsNode = responseBody.path("rows");

        if (!rowsNode.isArray()) {
            log.error("Resposta inesperada do DbExplorer: {}", root.toPrettyString());
            throw new IllegalStateException("DbExplorer não retornou array 'rows' na resposta.");
        }

        ArrayNode rows = (ArrayNode) rowsNode;
        log.info("DEBUG_PEDIDOS_CONF: total de linhas retornadas: {}", rows.size());

        List<String> cols = extractColumns(fieldsMetadata);
        log.info("DEBUG_PEDIDOS_CONF: colunas retornadas: {}", cols);

        int iNunota        = indexOf(cols, "NUNOTA");
        int iNumNota       = indexOf(cols, "NUMNOTA");
        int iNomeParc      = indexOf(cols, "NOMEPARC");
        int iStatus        = indexOf(cols, "STATUS_CONFERENCIA");
        int iCodVendedor   = indexOf(cols, "CODVEND");
        int iNomeVendedor  = indexOf(cols, "NOME_VENDEDOR");
        int iSeq           = indexOf(cols, "SEQUENCIA");
        int iCodProd       = indexOf(cols, "CODPROD");
        int iDescricao     = indexOf(cols, "DESCRICAO");
        int iUnidade       = indexOf(cols, "UNIDADE");

        int iQtdAtual      = indexOf(cols, "QTD_ATUAL");
        int iQtdEsperada   = indexOf(cols, "QTD_ESPERADA");
        int iQtdConferida  = indexOf(cols, "QTD_CONFERIDA");

        int iVlrUnit       = indexOf(cols, "VLRUNIT");
        int iVlrTot        = indexOf(cols, "VLRTOT");

        if (iNunota < 0 || iStatus < 0) {
            log.error("Campos obrigatórios não encontrados em fieldsMetadata: {}", cols);
            throw new IllegalStateException(
                    "Campos NUNOTA/STATUS_CONFERENCIA não encontrados na resposta do DbExplorer."
            );
        }

        // 🔥 AGRUPAR POR NUNOTA
        LinkedHashMap<Long, PedidoConferenciaDto> pedidosMap = new LinkedHashMap<>();

        for (JsonNode r : rows) {
            if (!r.isArray()) continue;

            Long nunota       = readLong(r, iNunota);
            Long numNota      = (iNumNota >= 0)      ? readLong(r, iNumNota)      : null;
            String nomeParc   = (iNomeParc >= 0)     ? readText(r, iNomeParc)     : null;
            String st         = readText(r, iStatus);
            Long codVendedor  = (iCodVendedor >= 0)  ? readLong(r, iCodVendedor)  : null;
            String nomeVend   = (iNomeVendedor >= 0) ? readText(r, iNomeVendedor) : null;

            Integer sequencia = readInt(r, iSeq);
            Long codProd      = readLong(r, iCodProd);
            String descricao  = readText(r, iDescricao);
            String unidade    = readText(r, iUnidade);

            Double qtdAtual      = readDouble(r, iQtdAtual);      // QTDNEG (cortado)
            Double qtdEsperada   = readDouble(r, iQtdEsperada);   // QTDCONFVOLPAD
            Double qtdConferida  = readDouble(r, iQtdConferida);  // QTDCONF

            Double vlrUnit    = readDouble(r, iVlrUnit);
            Double vlrTot     = readDouble(r, iVlrTot);

            // 👇 Pega a quantidade ORIGINAL da memória (antes do corte)
            Double qtdOriginal = conferenciaWorkflowService
                    .getQtdOriginalItem(nunota, sequencia, qtdAtual);

            // 🔥 LOG POR ITEM – agora mostrando qtdOriginal também
            log.info(
                    "DEBUG_ITEM_CONF nunota={} seq={} codProd={} | QTD_ORIGINAL={} QTD_ATUAL={} QTD_ESPERADA={} QTD_CONFERIDA={}",
                    nunota, sequencia, codProd, qtdOriginal, qtdAtual, qtdEsperada, qtdConferida
            );

            if (nunota == null || st == null) {
                continue;
            }

            // pega (ou cria) o pedido
            PedidoConferenciaDto pedido = pedidosMap.get(nunota);
            if (pedido == null) {
                pedido = new PedidoConferenciaDto(
                        nunota,
                        numNota,
                        nomeParc,
                        st,
                        codVendedor,
                        nomeVend,
                        null,   // nomeConferente
                        null    // avatarUrlConferente
                );
                pedidosMap.put(nunota, pedido);
            } else {
                // garante preenchimento de campos caso venham nulos em algumas linhas
                if (pedido.getNumNota() == null) {
                    pedido.setNumNota(numNota);
                }
                if (pedido.getNomeParc() == null) {
                    pedido.setNomeParc(nomeParc);
                }
                if (pedido.getCodVendedor() == null) {
                    pedido.setCodVendedor(codVendedor);
                }
                if (pedido.getNomeVendedor() == null) {
                    pedido.setNomeVendedor(nomeVend);
                }
            }

            // adiciona item com todas as quantidades
            pedido.addItem(new ItemConferenciaDto(
                    sequencia,
                    codProd,
                    descricao,
                    unidade,
                    qtdAtual,
                    qtdEsperada,
                    qtdConferida,
                    vlrUnit,
                    vlrTot,
                    qtdOriginal   // 👈 novo campo no DTO
            ));
        }

        List<PedidoConferenciaDto> listaFinal = new ArrayList<>(pedidosMap.values());
        log.info("DEBUG_PEDIDOS_CONF: total de pedidos montados: {}", listaFinal.size());
        return listaFinal;
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
