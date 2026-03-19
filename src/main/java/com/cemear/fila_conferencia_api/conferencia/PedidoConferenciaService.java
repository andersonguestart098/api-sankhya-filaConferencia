package com.cemear.fila_conferencia_api.conferencia;

import com.cemear.fila_conferencia_api.conferencia.mongo.PedidoConferenciaDoc;
import com.cemear.fila_conferencia_api.conferencia.mongo.PedidoConferenciaMongoService;
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
    private final ConferenciaWorkflowService conferenciaWorkflowService;
    private final PedidoConferenciaMongoService pedidoConferenciaMongoService;

    private static final String SQL_PEDIDOS_PAGINADO_COM_ESTOQUE_TEMPLATE = """
WITH BASE_PEDIDOS AS (
    SELECT
        CAB.NUNOTA,
        CAB.NUMNOTA,
        CAB.CODPARC,
        CAB.CODVEND,
        CAB.AD_RETIRA,
        CAB.CODEMP,
        CAB.NUCONFATUAL,
        SANKHYA.SNK_GET_SATUSCONFERENCIA(CAB.NUNOTA) AS STATUS_CONFERENCIA
    FROM TGFCAB CAB
    WHERE 1 = 1
      %s
),
PEDIDOS_COM_STATUS AS (
    SELECT
        B.NUNOTA,
        B.NUMNOTA,
        B.CODPARC,
        B.CODVEND,
        B.AD_RETIRA,
        B.CODEMP,
        B.NUCONFATUAL,
        B.STATUS_CONFERENCIA
    FROM BASE_PEDIDOS B
    WHERE NVL(TRIM(B.STATUS_CONFERENCIA), '#') <> '#'
),
PEDIDOS_NUMERADOS AS (
    SELECT
        P.NUNOTA,
        P.NUMNOTA,
        P.CODPARC,
        P.CODVEND,
        P.AD_RETIRA,
        P.CODEMP,
        P.NUCONFATUAL,
        P.STATUS_CONFERENCIA,
        ROW_NUMBER() OVER (ORDER BY P.NUNOTA DESC) AS RN
    FROM PEDIDOS_COM_STATUS P
),
PEDIDOS_PAGINADOS AS (
    SELECT
        PN.NUNOTA,
        PN.NUMNOTA,
        PN.CODPARC,
        PN.CODVEND,
        PN.AD_RETIRA,
        PN.CODEMP,
        PN.NUCONFATUAL,
        PN.STATUS_CONFERENCIA,
        PN.RN
    FROM PEDIDOS_NUMERADOS PN
    WHERE 1 = 1
      %s
),
ITENS_PAGINA AS (
    SELECT
        P.NUNOTA,
        P.NUMNOTA,
        P.CODPARC,
        P.CODVEND,
        P.AD_RETIRA,
        P.CODEMP,
        P.NUCONFATUAL,
        P.STATUS_CONFERENCIA,
        P.RN,

        ITE.SEQUENCIA,
        ITE.CODPROD,
        ITE.CODVOL,
        ITE.QTDNEG,
        ITE.VLRUNIT,
        ITE.VLRTOT,

        PRO.CODGRUPOPROD,
        PRO.DESCRPROD
    FROM PEDIDOS_PAGINADOS P
    JOIN TGFITE ITE
      ON ITE.NUNOTA = P.NUNOTA
    JOIN TGFPRO PRO
      ON PRO.CODPROD = ITE.CODPROD
    WHERE 1 = 1
      %s
),
CHAVES_ESTOQUE AS (
    SELECT DISTINCT
        I.CODPROD,
        I.CODEMP
    FROM ITENS_PAGINA I
),
EST_AGREGADO AS (
    SELECT
        EST.CODPROD,
        EST.CODEMP,
        SUM(NVL(EST.ESTOQUE, 0)) AS ESTOQUE_TOTAL,
        SUM(NVL(EST.RESERVADO, 0)) AS RESERVADO_TOTAL
    FROM TGFEST EST
    JOIN CHAVES_ESTOQUE C
      ON C.CODPROD = EST.CODPROD
     AND C.CODEMP  = EST.CODEMP
    GROUP BY
        EST.CODPROD,
        EST.CODEMP
)
SELECT
    I.NUNOTA,
    I.NUMNOTA,
    PAR.NOMEPARC,
    I.STATUS_CONFERENCIA,
    I.CODVEND,
    VEND.APELIDO AS NOME_VENDEDOR,
    I.AD_RETIRA AS TIPO_ENTREGA,

    I.SEQUENCIA,
    I.CODPROD,
    I.CODGRUPOPROD AS CODGRUPOPROD,
    I.DESCRPROD    AS DESCRICAO,
    I.CODVOL       AS UNIDADE,
    I.QTDNEG       AS QTD_ATUAL,

    COI.QTDCONFVOLPAD AS QTD_ESPERADA,
    COI.QTDCONF       AS QTD_CONFERIDA,

    I.VLRUNIT,
    I.VLRTOT,

    NVL(E.ESTOQUE_TOTAL, 0) AS ESTOQUE_TOTAL,
    NVL(E.RESERVADO_TOTAL, 0) AS ESTOQUE_RESERVADO,
    NVL(E.ESTOQUE_TOTAL, 0) - NVL(E.RESERVADO_TOTAL, 0) AS ESTOQUE_DISP
FROM ITENS_PAGINA I
JOIN TGFPAR PAR
  ON PAR.CODPARC = I.CODPARC
LEFT JOIN TGFVEN VEND
  ON VEND.CODVEND = I.CODVEND
LEFT JOIN TGFCOI2 COI
  ON COI.NUCONF  = I.NUCONFATUAL
 AND COI.SEQCONF = I.SEQUENCIA
LEFT JOIN EST_AGREGADO E
  ON E.CODPROD = I.CODPROD
 AND E.CODEMP  = I.CODEMP
ORDER BY
    I.NUNOTA DESC,
    I.SEQUENCIA
""";

    public List<PedidoConferenciaDto> listarPendentes() {
        return listarPendentesPaginado(0, 0, null, null, null, null);
    }

    public List<PedidoConferenciaDto> listarPendentesPaginado(
            int page,
            int pageSize,
            String status,
            LocalDate dataIni,
            LocalDate dataFim,
            Long nunotaFiltro
    ) {
        StringBuilder filtroData = new StringBuilder();

        if (dataIni != null && dataFim != null) {
            DateTimeFormatter fmt = DateTimeFormatter.ofPattern("dd/MM/yyyy");
            filtroData.append(String.format("""
        AND CAB.DTNEG >= TO_DATE('%s', 'DD/MM/YYYY')
        AND CAB.DTNEG < TO_DATE('%s', 'DD/MM/YYYY') + 1
    """, dataIni.format(fmt), dataFim.format(fmt)));
        } else {
            filtroData.append("""
        AND CAB.DTNEG >= TRUNC(SYSDATE) - 90
    """);
        }

        if (nunotaFiltro != null) {
            filtroData.append(" AND CAB.NUNOTA = ").append(nunotaFiltro).append(" ");
            log.info("🔍 FILTRO NUNOTA ATIVO: {}", nunotaFiltro);
        }

        // neste fluxo, backend não filtra por status por enquanto
        String filtroStatusSql = "";

        String trechoPaginacaoSql = "";
        if (pageSize > 0) {
            int inicio = page * pageSize + 1;
            int fim = inicio + pageSize - 1;
            trechoPaginacaoSql = " AND RN BETWEEN " + inicio + " AND " + fim + " ";
        }

        String sql = String.format(
                SQL_PEDIDOS_PAGINADO_COM_ESTOQUE_TEMPLATE,
                filtroData,
                trechoPaginacaoSql,
                filtroStatusSql
        );

        log.info("SQL FINAL GERADO (COM ESTOQUE):\n{}", sql);

        JsonNode root = gatewayClient.executeDbExplorer(sql);
        JsonNode rowsNode = root.path("responseBody").path("rows");
        JsonNode fieldsMetadata = root.path("responseBody").path("fieldsMetadata");

        log.info("rowsNode.isArray={} size={}",
                rowsNode.isArray(),
                rowsNode.isArray() ? rowsNode.size() : -1);

        List<String> cols = extractColumns(fieldsMetadata);
        log.info("cols={}", cols);

        if (!rowsNode.isArray()) {
            throw new IllegalStateException("DbExplorer não retornou array 'rows'.");
        }

        ArrayNode rows = (ArrayNode) rowsNode;

        int iNunota = indexOf(cols, "NUNOTA");
        int iNumNota = indexOf(cols, "NUMNOTA");
        int iNomeParc = indexOf(cols, "NOMEPARC");
        int iStatus = indexOf(cols, "STATUS_CONFERENCIA");
        int iCodVendedor = indexOf(cols, "CODVEND");
        int iNomeVendedor = indexOf(cols, "NOME_VENDEDOR");
        int iTipoEntrega = indexOf(cols, "TIPO_ENTREGA");

        int iSeq = indexOf(cols, "SEQUENCIA");
        int iCodProd = indexOf(cols, "CODPROD");
        int iCodGrupoProd = indexOf(cols, "CODGRUPOPROD");
        int iDescricao = indexOf(cols, "DESCRICAO");
        int iUnidade = indexOf(cols, "UNIDADE");

        int iQtdAtual = indexOf(cols, "QTD_ATUAL");
        int iQtdEsperada = indexOf(cols, "QTD_ESPERADA");
        int iQtdConferida = indexOf(cols, "QTD_CONFERIDA");

        int iVlrUnit = indexOf(cols, "VLRUNIT");
        int iVlrTot = indexOf(cols, "VLRTOT");

        int iEstoqueTotal = indexOf(cols, "ESTOQUE_TOTAL");
        int iEstoqueReservado = indexOf(cols, "ESTOQUE_RESERVADO");
        int iEstoqueDisp = indexOf(cols, "ESTOQUE_DISP");

        log.info("idx NUNOTA={}, NUMNOTA={}, STATUS={}, SEQUENCIA={}, CODPROD={}",
                iNunota, iNumNota, iStatus, iSeq, iCodProd);

        LinkedHashMap<Long, PedidoConferenciaDto> pedidosMap = new LinkedHashMap<>();

        int debugCount = 0;

        for (JsonNode r : rows) {
            if (!r.isArray()) continue;

            if (debugCount < 5) {
                log.info("row raw={}", r);
                log.info("nunota raw text={}", readText(r, iNunota));
                debugCount++;
            }

            Long nunota = readLong(r, iNunota);
            if (nunota == null) {
                log.warn("⚠️ nunota veio null. raw={}", readText(r, iNunota));
                continue;
            }

            String nomeParc = readText(r, iNomeParc);
            String st = readText(r, iStatus);
            Long codVend = readLong(r, iCodVendedor);
            String nomeVend = readText(r, iNomeVendedor);
            String tipoEntrega = readText(r, iTipoEntrega);
            Long numNota = readLong(r, iNumNota);

            Integer seq = readInt(r, iSeq);
            Long codProd = readLong(r, iCodProd);
            Long codGrupoProd = readLong(r, iCodGrupoProd);
            String desc = readText(r, iDescricao);
            String un = readText(r, iUnidade);

            Double qtdAtual = readDouble(r, iQtdAtual);
            Double qtdEsperada = readDouble(r, iQtdEsperada);
            Double qtdConferida = readDouble(r, iQtdConferida);

            Double vlrUnit = readDouble(r, iVlrUnit);
            Double vlrTot = readDouble(r, iVlrTot);

            Double estoqueTotal = readDouble(r, iEstoqueTotal);
            Double estoqueReservado = readDouble(r, iEstoqueReservado);
            Double estoqueDisp = readDouble(r, iEstoqueDisp);

            if (estoqueDisp == null && estoqueTotal != null && estoqueReservado != null) {
                estoqueDisp = estoqueTotal - estoqueReservado;
            } else if (estoqueDisp == null) {
                estoqueDisp = 0.0;
            }

            Double qtdOriginal = conferenciaWorkflowService.getQtdOriginalItem(
                    nunota,
                    seq,
                    qtdAtual
            );

            PedidoConferenciaDto pedido = pedidosMap.get(nunota);
            if (pedido == null) {
                pedido = new PedidoConferenciaDto(
                        nunota,
                        numNota,
                        nomeParc,
                        st,
                        codVend,
                        nomeVend,
                        tipoEntrega,
                        null,
                        null
                );


                pedidosMap.put(nunota, pedido);
            }

            pedido.addItem(new ItemConferenciaDto(
                    seq,
                    codProd,
                    codGrupoProd,
                    desc,
                    un,
                    qtdAtual,
                    qtdEsperada,
                    qtdConferida,
                    vlrUnit,
                    vlrTot,
                    qtdOriginal,
                    estoqueTotal,
                    estoqueDisp
            ));
        }

        try {
            List<Long> nunotas = new ArrayList<>(pedidosMap.keySet());
            if (!nunotas.isEmpty()) {
                var confMap = pedidoConferenciaMongoService.mapByNunota(nunotas);

                for (Long nunota : nunotas) {
                    PedidoConferenciaDto dto = pedidosMap.get(nunota);
                    PedidoConferenciaDoc doc = confMap.get(nunota);

                    if (dto != null && doc != null) {
                        if (doc.getConferenteNome() != null && !doc.getConferenteNome().trim().isEmpty()) {
                            dto.setNomeConferente(doc.getConferenteNome().trim());
                        }

                        dto.setTempoConferenciaMs(doc.getTempoConferenciaMs());
                    }
                }
            }
        } catch (Exception e) {
            log.warn("⚠️ Falha ao enriquecer conferente via Mongo. Mantendo payload original. Motivo={}", e.getMessage());
        }

        log.info("✅ pedidosMap.size()={}", pedidosMap.size());

        return new ArrayList<>(pedidosMap.values());
    }

    private static List<String> extractColumns(JsonNode fieldsMetadata) {
        List<String> cols = new ArrayList<>();
        for (JsonNode n : fieldsMetadata) {
            cols.add(n.path("name").asText());
        }
        return cols;
    }

    private static int indexOf(List<String> cols, String name) {
        for (int i = 0; i < cols.size(); i++) {
            if (cols.get(i).equalsIgnoreCase(name)) return i;
        }
        return -1;
    }

    private static String readText(JsonNode row, int idx) {
        if (idx < 0 || idx >= row.size()) return null;
        JsonNode v = row.get(idx);
        return v == null || v.isNull() ? null : v.asText();
    }

    private static Long readLong(JsonNode row, int idx) {
        try {
            if (idx < 0 || idx >= row.size()) return null;
            JsonNode v = row.get(idx);
            if (v == null || v.isNull()) return null;

            String txt = v.asText();
            if (txt == null) return null;

            txt = txt.trim();
            if (txt.isEmpty()) return null;

            txt = txt.replace(",", ".");

            if (txt.contains(".")) {
                double d = Double.parseDouble(txt);
                return (long) d;
            }

            return Long.valueOf(txt);
        } catch (Exception e) {
            return null;
        }
    }

    private static Integer readInt(JsonNode row, int idx) {
        try {
            if (idx < 0 || idx >= row.size()) return null;
            JsonNode v = row.get(idx);
            if (v == null || v.isNull()) return null;

            String txt = v.asText();
            if (txt == null) return null;

            txt = txt.trim();
            if (txt.isEmpty()) return null;

            txt = txt.replace(",", ".");

            if (txt.contains(".")) {
                double d = Double.parseDouble(txt);
                return (int) d;
            }

            return Integer.valueOf(txt);
        } catch (Exception e) {
            return null;
        }
    }

    private static Double readDouble(JsonNode row, int idx) {
        try {
            if (idx < 0 || idx >= row.size()) return null;
            JsonNode v = row.get(idx);
            if (v == null || v.isNull()) return null;

            String txt = v.asText();
            if (txt == null) return null;

            txt = txt.trim();
            if (txt.isEmpty()) return null;

            txt = txt.replace(",", ".");
            return Double.valueOf(txt);
        } catch (Exception e) {
            return null;
        }
    }
}