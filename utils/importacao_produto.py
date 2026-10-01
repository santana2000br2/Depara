"""
Importação Produto → Arquivo_Produto_Tratado → Produto_MG.

Carga Python na staging + pipeline de procedures legadas (up_01 a up_04) + De/Para.
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
    executar_pipeline_produto,
    obter_config_procedure,
)

PRODUTO_COLUNAS_CHAVE = frozenset({
    'CODIGO_PRODUTO', 'PRODUTO_REFERENCIA', 'PRODUTO_DESCRICAO',
    'VALOR_VENDA', 'CNPJ_EMPRESA',
})

TIPO_LAYOUT = 'produto'
_cfg = obter_config_procedure(TIPO_LAYOUT) or {}
TABELA_STAGING = _cfg.get('staging', 'Arquivo_Produto_Tratado')
TABELA_DESTINO = _cfg.get('destino', 'Produto_MG')


def layout_eh_produto(nome_layout, descricao=None, colunas=None):
    """
    Reconhece layout 7 Produto (não confundir com ProdutoEstoque).
    """
    for texto in (nome_layout, descricao):
        nome = _normalizar_nome_layout(texto)
        if not nome:
            continue
        if 'produtoestoque' in nome.replace('_', '') or 'produto_estoque' in nome:
            return False
        if 'prodlocacao' in nome.replace('_', '') or 'prod_locacao' in nome:
            return False
        if 'intercambiavel' in nome.replace('_', '') or 'intercambeavel' in nome.replace('_', ''):
            if texto == nome_layout:
                return False
            continue
        if nome == 'produto' or (nome.endswith('_produto') and 'estoque' not in nome):
            return True
        if 'produto' in nome and 'estoque' not in nome:
            if nome.startswith('produto') or ' produto' in f' {nome}':
                return True

    if colunas:
        nomes = set()
        for c in colunas:
            if isinstance(c, dict):
                nomes.add(str(c.get('Descricao') or '').strip().upper())
            else:
                nomes.add(str(c).strip().upper())
        nomes.discard('')
        if 'ESTOQUE_CODIGO' in nomes and 'QUANTIDADE' in nomes and 'PRODUTO_DESCRICAO' not in nomes:
            return False
        if PRODUTO_COLUNAS_CHAVE.issubset(nomes):
            return True

    return False


def importar_produto_para_base(df, banco_gx, banco_wf=None):
    """
    Importa DataFrame validado para DadosGX (Produto_MG).
    Retorna (sucesso, mensagem, resumo_dict).
    """
    from db.connection import conectar_segunda_base

    banco_gx = _validar_identificador_sql(banco_gx.strip())
    if not banco_wf:
        return False, "BancoHomo (BancoWF) é obrigatório para importação de Produto.", {}

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

        executar_pipeline_produto(cursor, banco_gx, banco_wf)

        from utils.importacao_depara_procedures import executar_depara_pos_importacao
        resumo_depara = executar_depara_pos_importacao(cursor, TIPO_LAYOUT, banco_gx, banco_wf)

        conn.commit()
        resumo = obter_resumo_importacao(cursor, banco_gx, TABELA_DESTINO)
        resumo['inseridos_staging'] = total_inserido
        resumo['procedure'] = _cfg.get('procedure', 'up_01_Extrai_Produto_gx')
        resumo['procedures'] = _cfg.get('procedures', [])
        resumo['depara'] = resumo_depara
        procs = ', '.join(resumo.get('procedures') or [resumo['procedure']])
        msg = (
            f"Importação concluída em {banco_gx}.dbo.{TABELA_DESTINO} "
            f"(procedures {procs}): "
            f"{resumo['total']} registro(s), {resumo['flag_1']} importado(s) OK, "
            f"{resumo['flag_0']} rejeitado(s)."
        )
        return True, msg, resumo
    except Exception as e:
        conn.rollback()
        logger.exception("Erro na importação Produto")
        return False, f"Erro na importação: {e}", {}
    finally:
        cursor.close()
        conn.close()
