package com.cemear.fila_conferencia_api.conferencia.cache;

import com.cemear.fila_conferencia_api.conferencia.PedidoConferenciaDto;
import com.cemear.fila_conferencia_api.conferencia.mongo.PedidoConferenciaDoc;
import com.cemear.fila_conferencia_api.conferencia.mongo.PedidoConferenciaMongoService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.lang.reflect.Method;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
@RequiredArgsConstructor
@Slf4j
public class PedidoConferenciaCacheService {

    private final PedidoConferenciaMongoService pedidoConferenciaMongoService;

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

        Stream<PedidoConferenciaDoc> stream = docs.stream()
                .filter(Objects::nonNull)
                .filter(doc -> doc.getSnapshot() != null);

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