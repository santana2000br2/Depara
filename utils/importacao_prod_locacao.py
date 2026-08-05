"""
Importação ProdLocacao → Arquivo_ProdLocacao_Tratado → ProdLocacao_MG.

Requer Produto_MG (layout 7 Produto) com a mesma PRODUTO_REFERENCIA + CNPJ_EMPRESA.
Sem De/Para.
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

PROD_LOCACAO_COLUNAS_CHAVE = frozenset({
    'PRODUTO_REFERENCIA', 'CNPJ_EMPRESA', 'LOC_PRIMARIA', 'LOC_SECUNDARIA',
})

TIPO_LAYOUT = 'prod_locacao'
_cfg = obter_config_procedure(TIPO_LAYOUT) or {}
TABELA_STAGING = _cfg.get('staging', 'Arquivo_ProdLocacao_Tratado')
TABELA_DESTINO = _cfg.get('destino', 'ProdLocacao_MG')


def layout_eh_prod_locacao(nome_layout, descricao=None, colunas=None):
    """Reconhece layout ProdLocacao (localização de produto no estoque)."""
    for texto in (nome_layout, descricao):
        nome = _normalizar_nome_layout(texto)
        if not nome:
            continue
        chave = nome.replace('_', '')
        if 'prodlocacao' in chave or 'prod_locacao' in nome:
            return True

    if colunas:
        nomes = set()
        for c in colunas:
            if isinstance(c, dict):
                nomes.add(str(c.get('Descricao') or '').strip().upper())
            else:
                nomes.add(str(c).strip().upper())
        nomes.discard('')
        if PROD_LOCACAO_COLUNAS_CHAVE.issubset(nomes):
            return True
        if (
            'PRODUTO_REFERENCIA' in nomes
            and 'CNPJ_EMPRESA' in nomes
            and ('LOC_PRIMARIA' in nomes or 'LOC_SECUNDARIA' in nomes)
            and 'ESTOQUE_CODIGO' not in nomes
            and 'QUANTIDADE' not in nomes
            and 'MOVIMENTO_CODIGO' not in nomes
        ):
            return True

    return False


def importar_prod_locacao_para_base(df, banco_gx, banco_wf=None):
    """Importa DataFrame validado para DadosGX (ProdLocacao_MG)."""
    from db.connection import conectar_segunda_base

    banco_gx = _validar_identificador_sql(banco_gx.strip())
    if not banco_wf:
        return False, "BancoHomo (BancoWF) é obrigatório para ProdLocacao.", {}

    banco_wf = _validar_identificador_sql(banco_wf.strip())

    if df is None or df.empty:
        return False, "Nenhum dado para importar.", {}

    df = _normalizar_colunas_dataframe(df)

    conn = conectar_segunda_base(banco_gx)
    if not conn:
        return False, f"Não foi possível conectar ao banco {banco_gx}.", {}

    cursor = conn.cursor()
    try:
        from utils.importacao_produto_mg_dependencia import validar_dataframe_depende_produto_mg
        validar_dataframe_depende_produto_mg(
            cursor, df,
            layout_nome=TIPO_LAYOUT,
            colunas_layout=[{'Descricao': c} for c in df.columns],
        )

        colunas_df = [c for c in df.columns if c not in ('IDtabela', 'Flag')]
        garantir_tabela_staging(cursor, banco_gx, colunas_df, TABELA_STAGING)
        total_inserido = inserir_staging(cursor, banco_gx, df, TABELA_STAGING)

        executar_procedure_extracao(cursor, TIPO_LAYOUT, banco_gx, banco_wf)

        conn.commit()
        resumo = obter_resumo_importacao(cursor, banco_gx, TABELA_DESTINO)
        resumo['inseridos_staging'] = total_inserido
        resumo['procedure'] = _cfg.get('procedure', 'up_01_Extrai_ProdLocacao_gx')
        msg = (
            f"Importação concluída em {banco_gx}.dbo.{TABELA_DESTINO} "
            f"(procedure {resumo['procedure']}): "
            f"{resumo['total']} registro(s), {resumo['flag_1']} importado(s) OK, "
            f"{resumo['flag_0']} rejeitado(s)."
        )
        return True, msg, resumo
    except Exception as e:
        conn.rollback()
        logger.exception("Erro na importação ProdLocacao")
        return False, f"Erro na importação: {e}", {}
    finally:
        cursor.close()
        conn.close()
