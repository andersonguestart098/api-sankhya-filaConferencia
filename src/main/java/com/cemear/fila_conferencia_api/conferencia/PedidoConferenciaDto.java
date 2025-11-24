package com.cemear.fila_conferencia_api.conferencia;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@Data
@NoArgsConstructor
public class PedidoConferenciaDto {

    private Long nunota;
    private String statusConferencia;

    // quem está conferindo (pro app)
    private String nomeConferente;
    private String avatarUrlConferente;

    private List<ItemConferenciaDto> itens = new ArrayList<>();

    public PedidoConferenciaDto(
            Long nunota,
            String statusConferencia,
            String nomeConferente,
            String avatarUrlConferente
    ) {
        this.nunota = nunota;
        this.statusConferencia = statusConferencia;
        this.nomeConferente = nomeConferente;
        this.avatarUrlConferente = avatarUrlConferente;
    }

    // helper pra ficar fácil adicionar item
    public void addItem(ItemConferenciaDto item) {
        this.itens.add(item);
    }
}
