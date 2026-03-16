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

    // ✅ NOVO: serviço que consulta o Mongo para enriquecer o DTO
    private final PedidoConferenciaMongoService pedidoConferenciaMongoService;

// =====================================================
// ✅ QUERY COM ESTOQUE CORRIGIDA - Agrega os valores de estoque
// =====================================================
private static final String SQL_PEDIDOS_PAGINADO_COM_ESTOQUE_TEMPLATE = """
SELECT
    X.NUNOTA,
    CAB.NUMNOTA,
    PAR.NOMEPARC,
    X.STATUS_CONFERENCIA,
    CAB.CODVEND,
    VEND.APELIDO AS NOME_VENDEDOR,
    CAB.AD_RETIRA AS TIPO_ENTREGA,

    ITE.SEQUENCIA,
    ITE.CODPROD,
    PRO.CODGRUPOPROD         AS CODGRUPOPROD,
    PRO.DESCRPROD            AS DESCRICAO,
    ITE.CODVOL               AS UNIDADE,
    ITE.QTDNEG               AS QTD_ATUAL,

    COI.QTDCONFVOLPAD        AS QTD_ESPERADA,
    COI.QTDCONF              AS QTD_CONFERIDA,

    ITE.VLRUNIT,
    ITE.VLRTOT,

    NVL(EST_AGREGADO.ESTOQUE_TOTAL, 0) AS ESTOQUE_TOTAL,
    NVL(EST_AGREGADO.RESERVADO_TOTAL, 0) AS ESTOQUE_RESERVADO,
    NVL(EST_AGREGADO.ESTOQUE_TOTAL, 0) - NVL(EST_AGREGADO.RESERVADO_TOTAL, 0) AS ESTOQUE_DISP
FROM (
    SELECT
        CAB.NUNOTA,
        CAB.CODPARC,
        SANKHYA.SNK_GET_SATUSCONFERENCIA(CAB.NUNOTA) AS STATUS_CONFERENCIA,
        ROW_NUMBER() OVER (ORDER BY CAB.NUNOTA DESC) AS RN
    FROM TGFCAB CAB
    WHERE 1 = 1
      %s
) X
JOIN TGFCAB CAB ON CAB.NUNOTA = X.NUNOTA
JOIN TGFPAR PAR ON PAR.CODPARC = CAB.CODPARC
JOIN TGFITE ITE ON ITE.NUNOTA = X.NUNOTA
JOIN TGFPRO PRO ON PRO.CODPROD = ITE.CODPROD
LEFT JOIN TGFVEN VEND ON VEND.CODVEND = CAB.CODVEND
LEFT JOIN TGFCOI2 COI ON COI.NUCONF = CAB.NUCONFATUAL AND COI.SEQCONF = ITE.SEQUENCIA

LEFT JOIN (
    SELECT 
        EST.CODPROD,
        EST.CODEMP,
        SUM(NVL(EST.ESTOQUE, 0)) AS ESTOQUE_TOTAL,
        SUM(NVL(EST.RESERVADO, 0)) AS RESERVADO_TOTAL
    FROM TGFEST EST
    GROUP BY EST.CODPROD, EST.CODEMP
) EST_AGREGADO ON EST_AGREGADO.CODPROD = ITE.CODPROD 
               AND EST_AGREGADO.CODEMP = CAB.CODEMP

WHERE NVL(X.STATUS_CONFERENCIA, ' ') <> ' '
  %s
  %s
ORDER BY X.NUNOTA DESC, ITE.SEQUENCIA
""";

    public List<PedidoConferenciaDto> listarPendentes() {
        return listarPendentesPaginado(0, 0, null, null, null, null);
    }

    /**
     * AGORA ACEITA NUNOTA E TRAZ ESTOQUE! (6 parâmetros)
     */
    public List<PedidoConferenciaDto> listarPendentesPaginado(
            int page,
            int pageSize,
            String status,
            LocalDate dataIni,
            LocalDate dataFim,
            Long nunotaFiltro
    ) {

        // --------------------- FILTRO DE DATA E NUNOTA ------------------------
        StringBuilder filtroData = new StringBuilder();

        if (dataIni != null && dataFim != null) {
            DateTimeFormatter fmt = DateTimeFormatter.ofPattern("dd/MM/yyyy");
            filtroData.append(String.format("""
                AND CAB.DTNEG >= TO_DATE('%s', 'DD/MM/YYYY')
                AND CAB.DTNEG < TO_DATE('%s', 'DD/MM/YYYY') + 1
            """, dataIni.format(fmt), dataFim.format(fmt)));
        } else {
            filtroData.append("""
                AND CAB.DTNEG >= TRUNC(SYSDATE, 'MM')
                AND CAB.DTNEG < ADD_MONTHS(TRUNC(SYSDATE, 'MM'), 1)
            """);
        }

        // 🔍 FILTRO POR NUNOTA
        if (nunotaFiltro != null) {
            filtroData.append(" AND CAB.NUNOTA = ").append(nunotaFiltro).append(" ");
            log.info("🔍 FILTRO NUNOTA ATIVO: {}", nunotaFiltro);
        }

        // --------------------- FILTRO STATUS ------------------------
        String filtroStatusSql = "";
        if (status != null && !status.isBlank()) {
            filtroStatusSql = " AND X.STATUS_CONFERENCIA = '" + status.trim() + "'";
        }

        // --------------------- PAGINAÇÃO ------------------------
        String trechoPaginacaoSql = "";
        if (pageSize > 0) {
            int inicio = page * pageSize + 1;
            int fim = inicio + pageSize - 1;

            trechoPaginacaoSql =
                    " AND X.RN BETWEEN " + inicio + " AND " + fim + " ";
        }

        // --------------------- MONTA SQL FINAL ------------------------
        String sql = String.format(
                SQL_PEDIDOS_PAGINADO_COM_ESTOQUE_TEMPLATE,
                filtroData,
                filtroStatusSql,
                trechoPaginacaoSql
        );

        log.info("SQL FINAL GERADO (COM ESTOQUE):\n{}", sql);

        // --------------------- EXECUÇÃO ------------------------
        JsonNode root = gatewayClient.executeDbExplorer(sql);
        JsonNode rowsNode = root.path("responseBody").path("rows");
        JsonNode fieldsMetadata = root.path("responseBody").path("fieldsMetadata");

        if (!rowsNode.isArray()) {
            throw new IllegalStateException("DbExplorer não retornou array 'rows'.");
        }

        ArrayNode rows = (ArrayNode) rowsNode;
        List<String> cols = extractColumns(fieldsMetadata);

        // Mapeamento dos índices das colunas
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

        // ✅ NOVOS ÍNDICES PARA ESTOQUE
        int iEstoqueTotal = indexOf(cols, "ESTOQUE_TOTAL");
        int iEstoqueReservado = indexOf(cols, "ESTOQUE_RESERVADO");
        int iEstoqueDisp = indexOf(cols, "ESTOQUE_DISP");

        LinkedHashMap<Long, PedidoConferenciaDto> pedidosMap = new LinkedHashMap<>();

        for (JsonNode r : rows) {
            if (!r.isArray()) continue;

            Long nunota = readLong(r, iNunota);
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

            // ✅ Lê os valores de estoque
            Double estoqueTotal = readDouble(r, iEstoqueTotal);
            Double estoqueReservado = readDouble(r, iEstoqueReservado);
            Double estoqueDisp = readDouble(r, iEstoqueDisp);

            // Se não conseguiu ler o estoque_disp, calcula
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

            // ✅ Adiciona o item com estoque
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
                    estoqueDisp
            ));
        }

        // ============================================================
        // ✅ ENRIQUECE NOME_CONFERENTE COM DADOS DO MONGO
        // ============================================================
        try {
            List<Long> nunotas = new ArrayList<>(pedidosMap.keySet());
            if (!nunotas.isEmpty()) {

                var confMap = pedidoConferenciaMongoService.mapByNunota(nunotas);

                for (Long nunota : nunotas) {
                    PedidoConferenciaDto dto = pedidosMap.get(nunota);
                    PedidoConferenciaDoc doc = confMap.get(nunota);

                    if (dto != null
                            && doc != null
                            && doc.getConferenteNome() != null
                            && !doc.getConferenteNome().trim().isEmpty()) {

                        dto.setNomeConferente(doc.getConferenteNome().trim());
                    }
                }
            }
        } catch (Exception e) {
            log.warn("⚠️ Falha ao enriquecer conferente via Mongo. Mantendo payload original. Motivo={}", e.getMessage());
        }

        return new ArrayList<>(pedidosMap.values());
    }

    // ----------------- HELPERS -------------------
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
        return v.isNull() ? null : v.asText();
    }

    private static Long readLong(JsonNode row, int idx) {
        try {
            if (idx < 0 || idx >= row.size()) return null;
            JsonNode v = row.get(idx);
            return v.isNull() ? null : Long.valueOf(v.asText().trim());
        } catch (Exception e) {
            return null;
        }
    }

    private static Integer readInt(JsonNode row, int idx) {
        try {
            if (idx < 0 || idx >= row.size()) return null;
            JsonNode v = row.get(idx);
            return v.isNull() ? null : Integer.valueOf(v.asText().trim());
        } catch (Exception e) {
            return null;
        }
    }

    private static Double readDouble(JsonNode row, int idx) {
        try {
            if (idx < 0 || idx >= row.size()) return null;
            JsonNode v = row.get(idx);
            return v.isNull() ? null : Double.valueOf(v.asText().replace(",", "."));
        } catch (Exception e) {
            return null;
        }
    }
}