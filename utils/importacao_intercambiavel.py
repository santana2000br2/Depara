"""
Importação Intercambiável → Arquivo_Intercambiavel_Tratado → ProdutoIntercambiavel_MG.

O legado up_13_Trata_Arquivo_Intercambiavel_gx lê Arquivo_Intercambiavel.conteudo.
Aqui o arquivo já vem validado em colunas; a publicação segue o script de
ProdutoIntercambiavel_MG (Flag=1 e ProdutoMarca_MarcaCod).
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
    executar_pipeline_intercambiavel,
    obter_config_procedure,
)

INTERCAMBIAVEL_COLUNAS_CHAVE = frozenset({
    'CODIGO_PRODUTO', 'PRODUTO_REFERENCIA',
})

TIPO_LAYOUT = 'intercambiavel'
_cfg = obter_config_procedure(TIPO_LAYOUT) or {}
TABELA_STAGING = _cfg.get('staging', 'Arquivo_Intercambiavel_Tratado')
TABELA_DESTINO = _cfg.get('destino', 'ProdutoIntercambiavel_MG')


def _texto_tem_intercambiavel(texto):
    nome = _normalizar_nome_layout(texto)
    if not nome:
        return False
    chave = nome.replace('_', '').replace('-', '').replace(' ', '')
    return 'intercambiavel' in chave or 'intercambeavel' in chave


def layout_eh_intercambiavel(nome_layout, descricao=None, colunas=None):
    """Reconhece layout 11-Intercambiavel (similaridade de produto)."""
    if _texto_tem_intercambiavel(nome_layout):
        return True

    if colunas:
        nomes = set()
        for c in colunas:
            if isinstance(c, dict):
                nomes.add(str(c.get('Descricao') or '').strip().upper())
            else:
                nomes.add(str(c).strip().upper())
        nomes.discard('')
        tem_inter = any('INTERCAMB' in n for n in nomes)
        if (
            tem_inter
            and INTERCAMBIAVEL_COLUNAS_CHAVE.issubset(nomes)
            and 'PRODUTO_DESCRICAO' not in nomes
            and 'VALOR_VENDA' not in nomes
        ):
            return True
    return False


def importar_intercambiavel_para_base(df, banco_gx, banco_wf=None):
    """Importa DataFrame validado para DadosGX (ProdutoIntercambiavel_MG)."""
    from db.connection import conectar_segunda_base

    banco_gx = _validar_identificador_sql(banco_gx.strip())
    if not banco_wf:
        return False, "BancoHomo (BancoWF) é obrigatório para Intercambiável.", {}

    banco_wf = _validar_identificador_sql(banco_wf.strip())

    if df is None or df.empty:
        return False, "Nenhum dado para importar.", {}

    df = _normalizar_colunas_dataframe(df)

    conn = conectar_segunda_base(banco_gx)
    if not conn:
        return False, f"Não foi possível conectar ao banco {banco_gx}.", {}

    cursor = conn.cursor()
    try:
        colunas_df = [c for c in df.columns if c not in ('IDtabela', 'Flag')]
        garantir_tabela_staging(cursor, banco_gx, colunas_df, TABELA_STAGING)
        total_inserido = inserir_staging(cursor, banco_gx, df, TABELA_STAGING)

        executar_pipeline_intercambiavel(cursor, banco_gx, banco_wf)

        conn.commit()
        resumo = obter_resumo_importacao(cursor, banco_gx, TABELA_DESTINO)
        resumo['inseridos_staging'] = total_inserido
        resumo['procedure'] = 'ProdutoIntercambiavel_MG (legado up_13 / script MG)'
        msg = (
            f"Importação concluída em {banco_gx}.dbo.{TABELA_DESTINO}: "
            f"{resumo['total']} registro(s), {resumo['flag_1']} importado(s) OK, "
            f"{resumo['flag_0']} rejeitado(s)."
        )
        return True, msg, resumo
    except Exception as e:
        conn.rollback()
        logger.exception("Erro na importação Intercambiável")
        return False, f"Erro na importação: {e}", {}
    finally:
        cursor.close()
        conn.close()
