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

    private static final String SQL_PEDIDOS_PAGINADO_TEMPLATE = """
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
        PRO.DESCRPROD            AS DESCRICAO,
        ITE.CODVOL               AS UNIDADE,
        ITE.QTDNEG               AS QTD_ATUAL,

        COI.QTDCONFVOLPAD        AS QTD_ESPERADA,
        COI.QTDCONF              AS QTD_CONFERIDA,

        ITE.VLRUNIT,
        ITE.VLRTOT
    FROM (
        SELECT
            CAB.NUNOTA,
            CAB.CODPARC,
            SANKHYA.SNK_GET_SATUSCONFERENCIA(CAB.NUNOTA) AS STATUS_CONFERENCIA,
            ROW_NUMBER() OVER (ORDER BY CAB.NUNOTA DESC) AS RN
        FROM TGFCAB CAB
        WHERE 1 = 1
          %s   -- filtro data + nunota
    ) X
    JOIN TGFCAB CAB ON CAB.NUNOTA = X.NUNOTA
    JOIN TGFPAR PAR ON PAR.CODPARC = CAB.CODPARC
    JOIN TGFITE ITE ON ITE.NUNOTA = X.NUNOTA
    JOIN TGFPRO PRO ON PRO.CODPROD = ITE.CODPROD
    LEFT JOIN TGFVEN VEND ON VEND.CODVEND = CAB.CODVEND
    LEFT JOIN TGFCOI2 COI ON COI.NUCONF = CAB.NUCONFATUAL AND COI.SEQCONF = ITE.SEQUENCIA

    WHERE NVL(X.STATUS_CONFERENCIA, ' ') <> ' '
      %s   -- filtro status
      %s   -- paginação

    ORDER BY X.NUNOTA DESC, ITE.SEQUENCIA
    """;

    public List<PedidoConferenciaDto> listarPendentes() {
        return listarPendentesPaginado(0, 0, null, null, null, null);
    }

    /**
     * AGORA ACEITA NUNOTA! (6 parâmetros)
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
                SQL_PEDIDOS_PAGINADO_TEMPLATE,
                filtroData,
                filtroStatusSql,
                trechoPaginacaoSql
        );

        log.info("SQL FINAL GERADO:\n{}", sql);

        // --------------------- EXECUÇÃO ------------------------
        JsonNode root = gatewayClient.executeDbExplorer(sql);
        JsonNode rowsNode = root.path("responseBody").path("rows");
        JsonNode fieldsMetadata = root.path("responseBody").path("fieldsMetadata");

        if (!rowsNode.isArray()) {
            throw new IllegalStateException("DbExplorer não retornou array 'rows'.");
        }

        ArrayNode rows = (ArrayNode) rowsNode;
        List<String> cols = extractColumns(fieldsMetadata);

        int iNunota = indexOf(cols, "NUNOTA");
        int iNumNota = indexOf(cols, "NUMNOTA");
        int iNomeParc = indexOf(cols, "NOMEPARC");
        int iStatus = indexOf(cols, "STATUS_CONFERENCIA");
        int iCodVendedor = indexOf(cols, "CODVEND");
        int iNomeVendedor = indexOf(cols, "NOME_VENDEDOR");

        // ✅ NOVO: campo que vem de CAB.AD_RETIRA AS TIPO_ENTREGA
        int iTipoEntrega = indexOf(cols, "TIPO_ENTREGA");

        int iSeq = indexOf(cols, "SEQUENCIA");
        int iCodProd = indexOf(cols, "CODPROD");
        int iDescricao = indexOf(cols, "DESCRICAO");
        int iUnidade = indexOf(cols, "UNIDADE");

        int iQtdAtual = indexOf(cols, "QTD_ATUAL");
        int iQtdEsperada = indexOf(cols, "QTD_ESPERADA");
        int iQtdConferida = indexOf(cols, "QTD_CONFERIDA");

        int iVlrUnit = indexOf(cols, "VLRUNIT");
        int iVlrTot = indexOf(cols, "VLRTOT");

        LinkedHashMap<Long, PedidoConferenciaDto> pedidosMap = new LinkedHashMap<>();

        for (JsonNode r : rows) {
            if (!r.isArray()) continue;

            Long nunota = readLong(r, iNunota);
            String nomeParc = readText(r, iNomeParc);
            String st = readText(r, iStatus);
            Long codVend = readLong(r, iCodVendedor);
            String nomeVend = readText(r, iNomeVendedor);

            // ✅ NOVO: lê o valor do AD_RETIRA (alias TIPO_ENTREGA)
            String tipoEntrega = readText(r, iTipoEntrega);

            Long numNota = readLong(r, iNumNota);

            Integer seq = readInt(r, iSeq);
            Long codProd = readLong(r, iCodProd);
            String desc = readText(r, iDescricao);
            String un = readText(r, iUnidade);

            Double qtdAtual = readDouble(r, iQtdAtual);
            Double qtdEsperada = readDouble(r, iQtdEsperada);
            Double qtdConferida = readDouble(r, iQtdConferida);

            Double vlrUnit = readDouble(r, iVlrUnit);
            Double vlrTot = readDouble(r, iVlrTot);

            Double qtdOriginal = conferenciaWorkflowService.getQtdOriginalItem(
                    nunota,
                    seq,
                    qtdAtual
            );

            PedidoConferenciaDto pedido = pedidosMap.get(nunota);
            if (pedido == null) {
                // ✅ Ajustado: agora passa o tipoEntrega no DTO (campo adRetira)
                pedido = new PedidoConferenciaDto(
                        nunota,
                        numNota,
                        nomeParc,
                        st,
                        codVend,
                        nomeVend,
                        tipoEntrega, // ✅ AQUI
                        null,
                        null
                );
                pedidosMap.put(nunota, pedido);
            }

            pedido.addItem(new ItemConferenciaDto(
                    seq,
                    codProd,
                    desc,
                    un,
                    qtdAtual,
                    qtdEsperada,
                    qtdConferida,
                    vlrUnit,
                    vlrTot,
                    qtdOriginal
            ));
        }

        // ============================================================
        // ✅ ENRIQUECE NOME_CONFERENTE COM DADOS DO MONGO (SEM QUEBRAR)
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
            // ⚠️ importante: não derruba produção se Mongo falhar
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
            return row.get(idx).isNull() ? null :
                    Long.valueOf(row.get(idx).asText().trim());
        } catch (Exception e) {
            return null;
        }
    }

    private static Integer readInt(JsonNode row, int idx) {
        try {
            return row.get(idx).isNull() ? null :
                    Integer.valueOf(row.get(idx).asText().trim());
        } catch (Exception e) {
            return null;
        }
    }

    private static Double readDouble(JsonNode row, int idx) {
        try {
            return row.get(idx).isNull() ? null :
                    Double.valueOf(row.get(idx).asText().replace(",", "."));
        } catch (Exception e) {
            return null;
        }
    }
}
