package com.cemear.fila_conferencia_api.conferencia;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@Data
@NoArgsConstructor
public class PedidoConferenciaDto {

    private Long nunota;                 // NUNOTA (chave interna Sankhya)
    private Long numNota;                // NUMNOTA (número da nota que aparece pro usuário)
    private String nomeParc;             // NOMEPARC (nome do cliente/parceiro)
    private String statusConferencia;    // status (AC, F, D, etc.)
    private List<ItemConferenciaDto> itens = new ArrayList<>();

    // construtor "novo" completo
    public PedidoConferenciaDto(Long nunota,
                                Long numNota,
                                String nomeParc,
                                String statusConferencia) {
        this.nunota = nunota;
        this.numNota = numNota;
        this.nomeParc = nomeParc;
        this.statusConferencia = statusConferencia;
    }

    // construtor antigo (compatibilidade, se já estava sendo usado)
    public PedidoConferenciaDto(Long nunota, String statusConferencia) {
        this(nunota, null, null, statusConferencia);
    }

    // helper pra ficar fácil adicionar item
    public void addItem(ItemConferenciaDto item) {
        this.itens.add(item);
    }
}
