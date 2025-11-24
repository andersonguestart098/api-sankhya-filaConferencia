// src/main/java/com/cemear/fila_conferencia_api/conferencia/PedidosPendentesScheduler.java
package com.cemear.fila_conferencia_api.conferencia;

import com.cemear.fila_conferencia_api.auth.PushService;
import com.cemear.fila_conferencia_api.auth.Usuario;
import com.cemear.fila_conferencia_api.auth.UsuarioRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Service
@RequiredArgsConstructor
@Slf4j
public class PedidosPendentesScheduler {

    private final PedidoConferenciaService pedidoConferenciaService;
    private final PushService pushService;
    private final UsuarioRepository usuarioRepository;

    // snapshot local das NUNOTAS já vistas (somente status AC)
    private Set<Long> ultimoSnapshot = new HashSet<>();

    @Scheduled(fixedDelay = 30000) // 30s
    public void verificarPedidosPendentes() {

        try {
            List<PedidoConferenciaDto> pendentes =
                    pedidoConferenciaService.listarPendentes();

            // monta o conjunto atual de NUNOTAS COM STATUS AC
            Set<Long> atual = new HashSet<>();
            for (PedidoConferenciaDto p : pendentes) {
                if (p.getNunota() != null
                        && "AC".equals(p.getStatusConferencia())) {
                    atual.add(p.getNunota());
                }
            }

            // novos itens = atual - ultimoSnapshot
            Set<Long> novos = new HashSet<>(atual);
            novos.removeAll(ultimoSnapshot);

            if (!novos.isEmpty()) {
                log.info("Novos pedidos (STATUS AC) encontrados: {}", novos);

                // busca todos usuários com pushToken preenchido
                List<Usuario> usuarios = usuarioRepository.findAll();

                for (Long nunota : novos) {
                    String titulo = "Novo pedido na fila";
                    String corpo  = "Pedido " + nunota + " aguardando conferência";

                    for (Usuario u : usuarios) {
                        String token = u.getPushToken();
                        if (token != null && !token.isBlank()) {
                            pushService.enviarPush(token, titulo, corpo);
                        }
                    }
                }
            }

            // atualiza snapshot só com os AC atuais
            ultimoSnapshot = atual;

        } catch (Exception e) {
            log.error("Erro no scheduler de pedidos pendentes: {}", e.getMessage(), e);
        }
    }
}
