package com.cemear.fila_conferencia_api.conferencia;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ItemConferenciaDto {

    private Integer sequencia;
    private Long codProd;
    private String descricao;
    private String unidade;
    private Double qtdNeg;
    private Double vlrUnit;
    private Double vlrTot;

}
