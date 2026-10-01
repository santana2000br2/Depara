"""Importação para o DadosGX sem depender de sessão Flask (jobs em segundo plano)."""

from logger import logger


def importar_por_tipo(tipo, df_processado, banco_gx, banco_wf):
    """
    Dispara a carga do layout já validado.
    Retorna (sucesso, mensagem, resumo) ou None se o tipo não for suportado.
    """
    from utils.importacao_forn_cli import importar_forn_cli_para_base
    from utils.importacao_forn_cli_documento import importar_forn_cli_documento_para_base
    from utils.importacao_forn_cli_endereco import importar_forn_cli_endereco_para_base
    from utils.importacao_forn_cli_enquadramento import importar_forn_cli_enquadramento_para_base
    from utils.importacao_forn_cli_telefone import importar_forn_cli_telefone_para_base
    from utils.importacao_forn_cli_contato import importar_forn_cli_contato_para_base
    from utils.importacao_produto import importar_produto_para_base
    from utils.importacao_produto_estoque import importar_produto_estoque_para_base
    from utils.importacao_prod_locacao import importar_prod_locacao_para_base
    from utils.importacao_movimento_estoque import importar_movimento_estoque_para_base
    from utils.importacao_veiculo import importar_veiculo_para_base
    from utils.importacao_fseg_cab import importar_fseg_cab_para_base
    from utils.importacao_fseg_prd import importar_fseg_prd_para_base
    from utils.importacao_fseg_srv import importar_fseg_srv_para_base
    from utils.importacao_financeiro import importar_financeiro_para_base
    from utils.importacao_adiantamento import importar_adiantamento_para_base
    from utils.importacao_intercambiavel import importar_intercambiavel_para_base

    if not tipo:
        return None

    if tipo == "forn_cli_documento":
        return importar_forn_cli_documento_para_base(df_processado, banco_gx, banco_wf)
    if tipo == "forn_cli_endereco":
        return importar_forn_cli_endereco_para_base(df_processado, banco_gx, banco_wf)
    if tipo == "forn_cli_enquadramento":
        return importar_forn_cli_enquadramento_para_base(df_processado, banco_gx, banco_wf)
    if tipo == "forn_cli_telefone":
        return importar_forn_cli_telefone_para_base(df_processado, banco_gx, banco_wf)
    if tipo == "forn_cli_contato":
        return importar_forn_cli_contato_para_base(df_processado, banco_gx, banco_wf)
    if tipo == "produto":
        return importar_produto_para_base(df_processado, banco_gx, banco_wf)
    if tipo == "produto_estoque":
        return importar_produto_estoque_para_base(df_processado, banco_gx, banco_wf)
    if tipo == "prod_locacao":
        return importar_prod_locacao_para_base(df_processado, banco_gx, banco_wf)
    if tipo == "movimento_estoque":
        return importar_movimento_estoque_para_base(df_processado, banco_gx, banco_wf)
    if tipo == "veiculo":
        return importar_veiculo_para_base(df_processado, banco_gx, banco_wf)
    if tipo == "fseg_cab":
        return importar_fseg_cab_para_base(df_processado, banco_gx, banco_wf)
    if tipo == "fseg_prd":
        return importar_fseg_prd_para_base(df_processado, banco_gx, banco_wf)
    if tipo == "fseg_srv":
        return importar_fseg_srv_para_base(df_processado, banco_gx, banco_wf)
    if tipo == "financeiro":
        return importar_financeiro_para_base(df_processado, banco_gx, banco_wf)
    if tipo == "adiantamento":
        return importar_adiantamento_para_base(df_processado, banco_gx, banco_wf)
    if tipo == "intercambiavel":
        return importar_intercambiavel_para_base(df_processado, banco_gx, banco_wf)
    if tipo == "forn_cli":
        return importar_forn_cli_para_base(df_processado, banco_gx, banco_wf)

    logger.warning("Tipo de importação não mapeado: %s", tipo)
    return None


def montar_resultado_importacao(tipo, cfg, banco_gx, sucesso, mensagem, resumo):
    from utils.importacao_depara_pessoa import formatar_resumo_depara

    resultado = {
        "sucesso": sucesso,
        "mensagem": mensagem,
        "resumo": resumo or {},
        "banco_gx": banco_gx,
        "automatica": True,
        "tipo": tipo,
        "tabela_destino": (cfg or {}).get("tabela_destino", ""),
        "procedure": (cfg or {}).get("procedure", ""),
        "resumo_texto": "",
        "depara_texto": "",
    }
    if sucesso:
        resultado["resumo_texto"] = (
            f"Total: {(resumo or {}).get('total', 0)} · "
            f"Importados OK: {(resumo or {}).get('flag_1', 0)} · "
            f"Rejeitados: {(resumo or {}).get('flag_0', 0)}"
        )
        resultado["flag_0"] = int((resumo or {}).get("flag_0") or 0)
        resultado["flag_1"] = int((resumo or {}).get("flag_1") or 0)
        resultado["total"] = int((resumo or {}).get("total") or 0)
        resultado["soma_estoque"] = (resumo or {}).get("soma_estoque") or []
        if (resumo or {}).get("depara"):
            resultado["depara_texto"] = "gerado"
            detalhe = formatar_resumo_depara(resumo["depara"])
            if detalhe:
                logger.info("De/Para gerado: %s", detalhe)
    return resultado
