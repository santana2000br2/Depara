"""Verificação de conclusão de blocos De/Para e disparo de e-mail."""

from logger import logger
from utils.depara_escopos import CATEGORIA_PARA_ESCOPO, CATEGORIAS_NOMES, ESCOPO_PARA_CATEGORIA
from utils.email_config import (
    bloco_ja_notificado,
    obter_smtp_config,
    registrar_notificacao_enviada,
    redefinir_notificacao_bloco,
    validar_lista_emails,
)
from utils.email_service import enviar_email, resolver_config_smtp


def _calcular_progresso_escopo(dados, escopo):
    categorias = ESCOPO_PARA_CATEGORIA.get(escopo, [])
    if not categorias:
        return 0, 0, 0

    total_qtd = 0
    total_concluido = 0

    for categoria in categorias:
        info = dados.get(categoria, {})
        if not isinstance(info, dict):
            continue
        qtd = info.get("qtd", 0)
        qtd_pendente = info.get("qtdPendente", 0)
        total_qtd += qtd
        total_concluido += qtd - qtd_pendente

    if total_qtd <= 0:
        return 0, total_qtd, total_concluido

    percentual = round((total_concluido / total_qtd) * 100, 1)
    return percentual, total_qtd, total_concluido


def _montar_corpo_email(nome_projeto, nome_bloco, percentual, total_qtd, total_concluido):
    assunto = f"[DE x PARA] Bloco {nome_bloco} concluído — {nome_projeto}"
    texto = (
        f"O bloco De/Para \"{nome_bloco}\" do projeto \"{nome_projeto}\" foi concluído.\n\n"
        f"Progresso: {percentual}%\n"
        f"Registros mapeados: {total_concluido} de {total_qtd}\n\n"
        "Acesse o sistema DE x PARA para revisar o dashboard do projeto."
    )
    html = (
        f"<p>O bloco De/Para <strong>{nome_bloco}</strong> do projeto "
        f"<strong>{nome_projeto}</strong> foi concluído.</p>"
        f"<p><strong>Progresso:</strong> {percentual}%<br>"
        f"<strong>Registros mapeados:</strong> {total_concluido} de {total_qtd}</p>"
        f"<p>Acesse o sistema DE x PARA para revisar o dashboard do projeto.</p>"
    )
    return assunto, texto, html


def verificar_notificacao_bloco(projeto_id, banco_usuario, escopo, nome_projeto, dados=None, config_email=None):
    """
    Verifica se o bloco atingiu 100% e envia e-mail se configurado.
    Retorna dict com status da verificação.
    """
    from routes.dashboard import obter_dados_por_categoria

    if not projeto_id or not banco_usuario or not escopo:
        return {"enviado": False, "motivo": "parametros_invalidos"}

    if config_email is None:
        config_email = obter_smtp_config(projeto_id)
    if not config_email or not config_email.get("ativo"):
        return {"enviado": False, "motivo": "notificacao_desativada"}

    destinatarios_txt = config_email.get("destinatarios", "")
    ok, emails = validar_lista_emails(destinatarios_txt)
    if not ok or not emails:
        return {"enviado": False, "motivo": "sem_destinatarios"}

    if dados is None:
        categorias = ESCOPO_PARA_CATEGORIA.get(escopo, [])
        dados = obter_dados_por_categoria(banco_usuario, categorias)

    percentual, total_qtd, total_concluido = _calcular_progresso_escopo(dados, escopo)

    if total_qtd <= 0:
        return {"enviado": False, "motivo": "sem_registros", "percentual": percentual}

    if percentual < 100:
        if bloco_ja_notificado(projeto_id, escopo):
            redefinir_notificacao_bloco(projeto_id, escopo)
        return {"enviado": False, "motivo": "incompleto", "percentual": percentual}

    if bloco_ja_notificado(projeto_id, escopo):
        return {"enviado": False, "motivo": "ja_enviado", "percentual": percentual}

    smtp_config = resolver_config_smtp(config_email)
    if not smtp_config:
        return {"enviado": False, "motivo": "smtp_nao_configurado", "percentual": percentual}

    nome_bloco = CATEGORIAS_NOMES.get(escopo, escopo)
    assunto, texto, html = _montar_corpo_email(
        nome_projeto, nome_bloco, percentual, total_qtd, total_concluido
    )

    try:
        enviar_email(smtp_config, emails, assunto, texto, html)
        registrar_notificacao_enviada(projeto_id, escopo, ", ".join(emails))
        return {
            "enviado": True,
            "bloco": escopo,
            "destinatarios": emails,
            "percentual": percentual,
        }
    except Exception as exc:
        logger.error(f"Erro ao enviar notificação do bloco {escopo}: {exc}")
        return {"enviado": False, "motivo": "erro_envio", "erro": str(exc), "percentual": percentual}


def verificar_notificacao_bloco_por_categoria(projeto_id, banco_usuario, categoria_chave, nome_projeto=None):
    escopo = CATEGORIA_PARA_ESCOPO.get(categoria_chave)
    if not escopo:
        return {"enviado": False, "motivo": "categoria_sem_escopo"}
    return verificar_notificacao_bloco(projeto_id, banco_usuario, escopo, nome_projeto or "Projeto")


def verificar_todos_blocos_projeto(projeto_id, banco_usuario, nome_projeto, escopos_habilitados=None, dados=None):
    from routes.dashboard import obter_dados_por_categoria, obter_escopos_projeto

    # Sem configuração/destinatários não há o que enviar; evita consultas pesadas à toa.
    config_email = obter_smtp_config(projeto_id)
    if not config_email or not config_email.get("ativo"):
        return []
    ok, emails = validar_lista_emails(config_email.get("destinatarios", ""))
    if not ok or not emails:
        return []

    if escopos_habilitados is None:
        escopos_habilitados = obter_escopos_projeto(projeto_id)

    if dados is None:
        categorias = []
        for escopo in escopos_habilitados:
            categorias.extend(ESCOPO_PARA_CATEGORIA.get(escopo, []))
        categorias = list(set(categorias))
        dados = obter_dados_por_categoria(banco_usuario, categorias)

    resultados = []
    for escopo in escopos_habilitados:
        if escopo not in CATEGORIAS_NOMES:
            continue
        resultado = verificar_notificacao_bloco(
            projeto_id, banco_usuario, escopo, nome_projeto,
            dados=dados, config_email=config_email,
        )
        resultado["bloco"] = escopo
        resultados.append(resultado)

    return resultados
