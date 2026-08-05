"""Hook chamado após commit de update_batch nas telas De/Para."""

from logger import logger


def apos_commit_batch_depara(categoria_chave, projeto_id, banco_usuario, nome_projeto=None):
    """Dispara verificação de conclusão do bloco sem interromper o fluxo do usuário."""
    if not categoria_chave or not projeto_id or not banco_usuario:
        return

    try:
        from utils.depara_notificacao import verificar_notificacao_bloco_por_categoria

        resultado = verificar_notificacao_bloco_por_categoria(
            projeto_id, banco_usuario, categoria_chave, nome_projeto
        )
        if resultado.get("enviado"):
            logger.info(
                f"Notificação de conclusão enviada para bloco "
                f"{resultado.get('bloco')} do projeto {projeto_id}"
            )
    except Exception as exc:
        logger.warning(f"Falha ao verificar notificação pós-De/Para ({categoria_chave}): {exc}")
