package com.cemear.fila_conferencia_api.conferencia.cache;

import com.cemear.fila_conferencia_api.conferencia.PedidoConferenciaDto;
import com.cemear.fila_conferencia_api.conferencia.mongo.PedidoConferenciaDoc;
import com.cemear.fila_conferencia_api.conferencia.mongo.PedidoConferenciaMongoService;
import com.cemear.fila_conferencia_api.sankhya.SankhyaGatewayClient;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.lang.reflect.Method;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
@RequiredArgsConstructor
@Slf4j
public class PedidoConferenciaCacheService {

    private final PedidoConferenciaMongoService pedidoConferenciaMongoService;
    private final SankhyaGatewayClient gatewayClient;

    public List<PedidoConferenciaDto> listarDoCache(
            int page,
            int pageSize,
            String status,
            LocalDate dataIni,
            LocalDate dataFim,
            Long nunota
    ) {
        List<PedidoConferenciaDto> todos = filtrar(status, dataIni, dataFim, nunota);

        int pagina = Math.max(page, 0);
        int tamanho = Math.max(pageSize, 1);

        int from = pagina * tamanho;
        if (from >= todos.size()) {
            return List.of();
        }

        int to = Math.min(todos.size(), from + tamanho);
        return todos.subList(from, to);
    }

    public long contarDoCache(
            String status,
            LocalDate dataIni,
            LocalDate dataFim,
            Long nunota
    ) {
        return filtrar(status, dataIni, dataFim, nunota).size();
    }

    private List<PedidoConferenciaDto> filtrar(
            String status,
            LocalDate dataIni,
            LocalDate dataFim,
            Long nunota
    ) {
        List<PedidoConferenciaDoc> docs = pedidoConferenciaMongoService.findAllSnapshot();

        List<Long> nunotasCache = docs.stream()
                .filter(Objects::nonNull)
                .map(PedidoConferenciaDoc::getNunota)
                .filter(Objects::nonNull)
                .distinct()
                .collect(Collectors.toList());

        Set<Long> nunotasValidas = buscarNunotasValidasNoSankhya(nunotasCache);

        Stream<PedidoConferenciaDoc> stream = docs.stream()
                .filter(Objects::nonNull)
                .filter(doc -> doc.getSnapshot() != null)
                .filter(doc -> doc.getNunota() != null)
                .filter(doc -> nunotasValidas.contains(doc.getNunota()));

        if (nunota != null) {
            stream = stream.filter(doc -> Objects.equals(doc.getNunota(), nunota));
        }

        if (status != null && !status.isBlank()) {
            String statusNorm = status.trim().toUpperCase(Locale.ROOT);
            stream = stream.filter(doc -> statusNorm.equals(normalize(doc.getLastStatusConferencia())));
        }

        if (dataIni != null) {
            stream = stream.filter(doc -> {
                LocalDate dt = extrairDataNegociacao(doc.getSnapshot());
                return dt != null && !dt.isBefore(dataIni);
            });
        }

        if (dataFim != null) {
            stream = stream.filter(doc -> {
                LocalDate dt = extrairDataNegociacao(doc.getSnapshot());
                return dt != null && !dt.isAfter(dataFim);
            });
        }

        return stream
                .sorted(Comparator
                        .comparing(
                                (PedidoConferenciaDoc doc) -> extrairDataNegociacao(doc.getSnapshot()),
                                Comparator.nullsLast(Comparator.reverseOrder())
                        )
                        .thenComparing(
                                PedidoConferenciaDoc::getNunota,
                                Comparator.nullsLast(Comparator.reverseOrder())
                        ))
                .map(PedidoConferenciaDoc::getSnapshot)
                .collect(Collectors.toList());
    }

    private Set<Long> buscarNunotasValidasNoSankhya(List<Long> nunotas) {
        Set<Long> validas = new HashSet<>();

        if (nunotas == null || nunotas.isEmpty()) {
            return validas;
        }

        final int lote = 200;

        for (int i = 0; i < nunotas.size(); i += lote) {
            List<Long> subLista = nunotas.subList(i, Math.min(i + lote, nunotas.size()));

            String inSql = subLista.stream()
                    .map(String::valueOf)
                    .collect(Collectors.joining(","));

            String sql = """
                SELECT CAB.NUNOTA
                FROM TGFCAB CAB
                WHERE CAB.NUNOTA IN (%s)
            """.formatted(inSql);

            try {
                JsonNode root = gatewayClient.executeDbExplorer(sql);
                JsonNode rowsNode = root.path("responseBody").path("rows");

                if (rowsNode.isArray()) {
                    ArrayNode rows = (ArrayNode) rowsNode;

                    for (JsonNode row : rows) {
                        if (row != null && row.isArray() && row.size() > 0) {
                            Long nunota = readLong(row, 0);
                            if (nunota != null) {
                                validas.add(nunota);
                            }
                        }
                    }
                }
            } catch (Exception e) {
                log.error("Erro ao validar lote de pedidos no Sankhya: {}", e.getMessage(), e);
            }
        }

        return validas;
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

    private String normalize(String value) {
        return value == null ? "" : value.trim().toUpperCase(Locale.ROOT);
    }

    private LocalDate extrairDataNegociacao(PedidoConferenciaDto dto) {
        if (dto == null) {
            return null;
        }

        Object value = invokeGetter(dto, "getDtNeg");
        if (value == null) value = invokeGetter(dto, "getDataNeg");
        if (value == null) value = invokeGetter(dto, "getDtneg");

        if (value == null) {
            return null;
        }

        if (value instanceof LocalDate localDate) {
            return localDate;
        }

        if (value instanceof java.util.Date date) {
            return date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
        }

        if (value instanceof java.time.Instant instant) {
            return instant.atZone(ZoneId.systemDefault()).toLocalDate();
        }

        if (value instanceof String s) {
            try {
                return LocalDate.parse(s);
            } catch (Exception ignored) {
                return null;
            }
        }

        return null;
    }

    private Object invokeGetter(Object target, String methodName) {
        try {
            Method method = target.getClass().getMethod(methodName);
            return method.invoke(target);
        } catch (Exception e) {
            return null;
        }
    }
}