package com.cemear.fila_conferencia_api.conferencia;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class ItemConferenciaDto {

    private Integer sequencia;
    private Long codProd;
    private String descricao;
    private String unidade;

    // Quantidades
    private Double qtdAtual;      // TGFITE.QTDNEG (depois do corte)
    private Double qtdEsperada;   // TGFCOI2.QTDCONFVOLPAD (antes do corte)
    private Double qtdConferida;  // TGFCOI2.QTDCONF (digitado no app)
    private Double qtdOriginal;
    private Double estoqueDisponivel;


    // Valores
    private Double vlrUnit;
    private Double vlrTot;

    // 👇 construtor “completão” usado no PedidoConferenciaService
    public ItemConferenciaDto(
            Integer sequencia,
            Long codProd,
            String descricao,
            String unidade,
            Double qtdAtual,
            Double qtdEsperada,
            Double qtdConferida,
            Double vlrUnit,
            Double vlrTot,
            Double qtdOriginal,
            Double estoqueDisponivel
    )
    {
        this.sequencia = sequencia;
        this.codProd = codProd;
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
