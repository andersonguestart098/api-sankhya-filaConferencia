package com.cemear.fila_conferencia_api.conferencia;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class PedidoConferenciaDto {

    private Long nunota;
    private String statusConferencia;
    private Integer sequencia;
    private Long codProd;
    private Double qtdNeg;
    private Double vlrUnit;
    private Double vlrTot;

    // construtor simples (caso você ainda use só nunota + status em algum lugar)
    public PedidoConferenciaDto(Long nunota, String statusConferencia) {
        this.nunota = nunota;
        this.statusConferencia = statusConferencia;
    }

    // construtor completo (usado no service com itens)
    public PedidoConferenciaDto(Long nunota,
                                String statusConferencia,
                                Integer sequencia,
                                Long codProd,
                                Double qtdNeg,
                                Double vlrUnit,
                                Double vlrTot) {
        this.nunota = nunota;
        this.statusConferencia = statusConferencia;
        this.sequencia = sequencia;
        this.codProd = codProd;
        this.qtdNeg = qtdNeg;
        this.vlrUnit = vlrUnit;
        this.vlrTot = vlrTot;
    }
}
