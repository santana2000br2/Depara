"""
Importação MovimentoEstoque → Arquivo_MovimentoEstoque_Tratado → MovimentoEstoque_MG.

CPF/CNPJ do comprador pode ser vazio ou placeholder (consumidor / cupom).
Quando informado CPF real, exige cadastro prévio em Pessoa_MG (layout 1 Forn_cli).
"""
from logger import logger
from utils.importacao_forn_cli import (
    _normalizar_colunas_dataframe,
    _normalizar_nome_layout,
    _validar_identificador_sql,
    garantir_tabela_staging,
    inserir_staging,
    obter_resumo_importacao,
)
from utils.importacao_procedures import (
    executar_procedure_extracao,
    obter_config_procedure,
)

MOVIMENTO_ESTOQUE_COLUNAS_CHAVE = frozenset({
    'MOVIMENTO_CODIGO', 'DATA_MOVIMENTO', 'CPF_CNPJ', 'CNPJ_EMPRESA', 'QUANTIDADE',
})

TIPO_LAYOUT = 'movimento_estoque'
_cfg = obter_config_procedure(TIPO_LAYOUT) or {}
TABELA_STAGING = _cfg.get('staging', 'Arquivo_MovimentoEstoque_Tratado')
TABELA_DESTINO = _cfg.get('destino', 'MovimentoEstoque_MG')


def layout_eh_movimento_estoque(nome_layout, descricao=None, colunas=None):
    """Reconhece layout MovimentoEstoque (não confundir com ProdutoEstoque)."""
    for texto in (nome_layout, descricao):
        nome = _normalizar_nome_layout(texto)
        if not nome:
            continue
        chave = nome.replace('_', '')
        if 'movimentoestoque' in chave or 'movimento_estoque' in nome:
            return True

    if colunas:
        nomes = set()
        for c in colunas:
            if isinstance(c, dict):
                nomes.add(str(c.get('Descricao') or '').strip().upper())
            else:
                nomes.add(str(c).strip().upper())
        nomes.discard('')
        if MOVIMENTO_ESTOQUE_COLUNAS_CHAVE.issubset(nomes):
            return True
        if (
            'MOVIMENTO_CODIGO' in nomes
            and 'DATA_MOVIMENTO' in nomes
            and 'CPF_CNPJ' in nomes
            and 'QUANTIDADE' in nomes
            and 'PRECO_MEDIO' not in nomes
            and 'PRODUTO_DESCRICAO' not in nomes
        ):
            return True

    return False


def importar_movimento_estoque_para_base(df, banco_gx, banco_wf=None):
    """Importa DataFrame validado para DadosGX (MovimentoEstoque_MG)."""
    from db.connection import conectar_segunda_base

    banco_gx = _validar_identificador_sql(banco_gx.strip())
    if not banco_wf:
        return False, "BancoHomo (BancoWF) é obrigatório para MovimentoEstoque.", {}

    banco_wf = _validar_identificador_sql(banco_wf.strip())

    if df is None or df.empty:
        return False, "Nenhum dado para importar.", {}

    df = _normalizar_colunas_dataframe(df)

    conn = conectar_segunda_base(banco_gx)
    if not conn:
        return False, f"Não foi possível conectar ao banco {banco_gx}.", {}

    cursor = conn.cursor()
    try:
        from utils.importacao_pessoa_mg_dependencia import validar_dataframe_depende_pessoa_mg
        validar_dataframe_depende_pessoa_mg(
            cursor, df,
            layout_nome=TIPO_LAYOUT,
            colunas_layout=[{'Descricao': c} for c in df.columns],
        )

        colunas_df = [c for c in df.columns if c not in ('IDtabela', 'Flag')]
        garantir_tabela_staging(cursor, banco_gx, colunas_df, TABELA_STAGING)
        total_inserido = inserir_staging(cursor, banco_gx, df, TABELA_STAGING)

        executar_procedure_extracao(cursor, TIPO_LAYOUT, banco_gx, banco_wf)

        from utils.importacao_depara_procedures import executar_depara_pos_importacao
        resumo_depara = executar_depara_pos_importacao(cursor, TIPO_LAYOUT, banco_gx, banco_wf)

        conn.commit()
        resumo = obter_resumo_importacao(cursor, banco_gx, TABELA_DESTINO)
        resumo['inseridos_staging'] = total_inserido
        resumo['procedure'] = _cfg.get('procedure', 'up_01_Extrai_MovimentoEstoque_gx')
        resumo['depara'] = resumo_depara
        msg = (
            f"Importação concluída em {banco_gx}.dbo.{TABELA_DESTINO} "
            f"(procedure {resumo['procedure']}): "
            f"{resumo['total']} registro(s), {resumo['flag_1']} importado(s) OK, "
            f"{resumo['flag_0']} rejeitado(s)."
        )
        return True, msg, resumo
    except Exception as e:
        conn.rollback()
        logger.exception("Erro na importação MovimentoEstoque")
        return False, f"Erro na importação: {e}", {}
    finally:
        cursor.close()
        conn.close()
