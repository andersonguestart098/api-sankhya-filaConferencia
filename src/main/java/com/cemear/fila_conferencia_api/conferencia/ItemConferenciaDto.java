package com.cemear.fila_conferencia_api.conferencia;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class ItemConferenciaDto {

    private Integer sequencia;
    private Long codProd;
    private Long codGrupoProd;
    private String descricao;
    private String unidade;

    // Quantidades
    private Double qtdAtual;      // TGFITE.QTDNEG
    private Double qtdEsperada;   // TGFCOI2.QTDCONFVOLPAD
    private Double qtdConferida;  // TGFCOI2.QTDCONF
    private Double qtdOriginal;
    private Double estoqueDisponivel;

    // Valores
    private Double vlrUnit;
    private Double vlrTot;

    // ✅ Construtor SEM estoque
    public ItemConferenciaDto(
            Integer sequencia,
            Long codProd,
            Long codGrupoProd,
            String descricao,
            String unidade,
            Double qtdAtual,
            Double qtdEsperada,
            Double qtdConferida,
            Double vlrUnit,
            Double vlrTot,
            Double qtdOriginal
    ) {
        this(
                sequencia,
                codProd,
                codGrupoProd,
                descricao,
                unidade,
                qtdAtual,
                qtdEsperada,
                qtdConferida,
                vlrUnit,
                vlrTot,
                qtdOriginal,
                null
        );
    }

    // ✅ Construtor COM estoque
    public ItemConferenciaDto(
            Integer sequencia,
            Long codProd,
            Long codGrupoProd,
            String descricao,
            String unidade,
            Double qtdAtual,
            Double qtdEsperada,
            Double qtdConferida,
            Double vlrUnit,
            Double vlrTot,
            Double qtdOriginal,
            Double estoqueDisponivel
    ) {
        this.sequencia = sequencia;
        this.codProd = codProd;
        this.codGrupoProd = codGrupoProd;
        this.descricao = descricao;
        this.unidade = unidade;
        this.qtdAtual = qtdAtual;
        this.qtdEsperada = qtdEsperada;
        this.qtdConferida = qtdConferida;
        this.vlrUnit = vlrUnit;
        this.vlrTot = vlrTot;
        this.qtdOriginal = qtdOriginal;
        this.estoqueDisponivel = estoqueDisponivel;
    }
}