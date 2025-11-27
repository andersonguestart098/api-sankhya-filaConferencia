package com.cemear.fila_conferencia_api.conferencia;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@Data
@NoArgsConstructor
public class PedidoConferenciaDto {

    // --------- CAMPOS BÁSICOS ---------
    private Long nunota;              // NUNOTA (chave interna Sankhya)
    private Long numNota;             // NUMNOTA (número da nota que aparece pro usuário)
    private String nomeParc;          // NOMEPARC (nome do cliente/parceiro)
    private String statusConferencia; // status (AC, F, D, etc.)

    // --------- CAMPOS DO VENDEDOR ---------
    private Long codVendedor;         // CODVEND
    private String nomeVendedor;      // APELIDO / NOME VENDEDOR

    // --------- CAMPOS DO CONFERENTE (DASH / APP) ---------
    private String nomeConferente;       // nome de quem está conferindo
    private String avatarUrlConferente;  // URL do avatar (se tiver)

    // --------- ITENS ---------
    private List<ItemConferenciaDto> itens = new ArrayList<>();

    // ==============================
    // CONSTRUTOR "COMPLETÃO" NOVO
    // ==============================
    public PedidoConferenciaDto(Long nunota,
                                Long numNota,
                                String nomeParc,
                                String statusConferencia,
                                Long codVendedor,
                                String nomeVendedor,
                                String nomeConferente,
                                String avatarUrlConferente) {
        this.nunota = nunota;
        this.numNota = numNota;
        this.nomeParc = nomeParc;
        this.statusConferencia = statusConferencia;
        this.codVendedor = codVendedor;
        this.nomeVendedor = nomeVendedor;
        this.nomeConferente = nomeConferente;
        this.avatarUrlConferente = avatarUrlConferente;
    }

    // ==============================
    // CONSTRUTOR "ANTIGO" (4 CAMPOS)
    // continua funcionando, só preenche o resto com null
    // ==============================
    public PedidoConferenciaDto(Long nunota,
                                Long numNota,
                                String nomeParc,
                                String statusConferencia) {
        this(nunota, numNota, nomeParc, statusConferencia,
                null, null, null, null);
    }

    // ==============================
    // CONSTRUTOR MAIS ANTIGO (2 CAMPOS)
    // continua aceitando só nunota + status
    // ==============================
    public PedidoConferenciaDto(Long nunota, String statusConferencia) {
        this(nunota, null, null, statusConferencia,
                null, null, null, null);
    }

    // helper pra ficar fácil adicionar item
    public void addItem(ItemConferenciaDto item) {
        this.itens.add(item);
    }
}
