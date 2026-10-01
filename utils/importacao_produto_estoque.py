"""
Importação ProdutoEstoque → Arquivo_ProdutoEstoque_Tratado → ProdutoEstoque_MG.

Requer Produto_MG (layout 7 Produto) com a mesma PRODUTO_REFERENCIA + CNPJ_EMPRESA.
"""
from logger import logger
from utils.importacao_forn_cli import (
    _coluna_existe,
    _executar,
    _normalizar_colunas_dataframe,
    _normalizar_nome_layout,
    _quote_col,
    _quote_table,
    _resolver_coluna,
    _tabela_existe,
    _validar_identificador_sql,
    garantir_tabela_staging,
    inserir_staging,
    obter_resumo_importacao,
)
from utils.importacao_procedures import (
    executar_procedure_extracao,
    obter_config_procedure,
)

PRODUTO_ESTOQUE_COLUNAS_CHAVE = frozenset({
    'PRODUTO_REFERENCIA', 'CNPJ_EMPRESA', 'ESTOQUE_CODIGO', 'QUANTIDADE',
})

TIPO_LAYOUT = 'produto_estoque'
_cfg = obter_config_procedure(TIPO_LAYOUT) or {}
TABELA_STAGING = _cfg.get('staging', 'Arquivo_ProdutoEstoque_Tratado')
TABELA_DESTINO = _cfg.get('destino', 'ProdutoEstoque_MG')


def layout_eh_produto_estoque(nome_layout, descricao=None, colunas=None):
    """Reconhece layout 8 ProdutoEstoque (não confundir com Produto)."""
    for texto in (nome_layout, descricao):
        nome = _normalizar_nome_layout(texto)
        if not nome:
            continue
        if 'produtoestoque' in nome.replace('_', '') or 'produto_estoque' in nome:
            return True
        if 'prodlocacao' in nome.replace('_', '') or 'prod_locacao' in nome:
            return False

    if colunas:
        nomes = set()
        for c in colunas:
            if isinstance(c, dict):
                nomes.add(str(c.get('Descricao') or '').strip().upper())
            else:
                nomes.add(str(c).strip().upper())
        nomes.discard('')
        if PRODUTO_ESTOQUE_COLUNAS_CHAVE.issubset(nomes):
            return True
        if (
            'ESTOQUE_CODIGO' in nomes
            and 'QUANTIDADE' in nomes
            and 'PRODUTO_REFERENCIA' in nomes
            and 'PRODUTO_DESCRICAO' not in nomes
            and 'MOVIMENTO_CODIGO' not in nomes
            and 'DATA_MOVIMENTO' not in nomes
        ):
            return True

    return False


def resumir_soma_estoque(cursor):
    """
    Totais de QUANTIDADE e PRECO_MEDIO por CNPJ_EMPRESA e ESTOQUE_CODIGO
    em ProdutoEstoque_MG, depois da importação.
    """
    colunas = ('CNPJ_EMPRESA', 'ESTOQUE_CODIGO', 'QUANTIDADE', 'PRECO_MEDIO')
    if not _tabela_existe(cursor, TABELA_DESTINO):
        return []
    if any(not _coluna_existe(cursor, TABELA_DESTINO, nome) for nome in colunas):
        logger.warning("Soma de estoque ignorada: coluna ausente em %s", TABELA_DESTINO)
        return []

    cnpj = _quote_col(_resolver_coluna(cursor, TABELA_DESTINO, 'CNPJ_EMPRESA'))
    estoque = _quote_col(_resolver_coluna(cursor, TABELA_DESTINO, 'ESTOQUE_CODIGO'))
    qtd = _quote_col(_resolver_coluna(cursor, TABELA_DESTINO, 'QUANTIDADE'))
    preco = _quote_col(_resolver_coluna(cursor, TABELA_DESTINO, 'PRECO_MEDIO'))
    dest = _quote_table(TABELA_DESTINO)
    qtd_num = f"TRY_CONVERT(decimal(18,4), REPLACE({qtd}, ',', '.'))"
    preco_num = f"TRY_CONVERT(decimal(18,4), REPLACE({preco}, ',', '.'))"

    _executar(cursor, f"""
        SELECT
            {cnpj},
            {estoque},
            FORMAT(SUM({qtd_num}), 'N0', 'pt-BR') AS QUANTIDADE_TOTAL,
            FORMAT(SUM({preco_num}), 'C', 'pt-BR') AS PRECO_MEDIO_TOTAL
        FROM dbo.{dest}
        WHERE {qtd_num} <> 0
        GROUP BY {cnpj}, {estoque}
        ORDER BY {cnpj}, {estoque}
    """)
    linhas = []
    for row in cursor.fetchall():
        linhas.append({
            'cnpj_empresa': '' if row[0] is None else str(row[0]).strip(),
            'estoque_codigo': '' if row[1] is None else str(row[1]).strip(),
            'quantidade_total': '' if row[2] is None else str(row[2]).strip(),
            'preco_medio_total': '' if row[3] is None else str(row[3]).strip(),
        })
    logger.info("Soma de estoque: %s grupo(s) CNPJ/ESTOQUE_CODIGO", len(linhas))
    return linhas


def corrigir_depara_estoque_codigo(cursor):
    """
    Estoque_DePara deve nascer de ESTOQUE_CODIGO (posição 4).
    A procedure antiga gravava LOCALIZACAO (posição 10) em est_ds.
    """
    if not _tabela_existe(cursor, 'Estoque_DePara') or not _tabela_existe(cursor, TABELA_DESTINO):
        return
    if not _coluna_existe(cursor, TABELA_DESTINO, 'ESTOQUE_CODIGO'):
        return

    dest = _quote_table(TABELA_DESTINO)
    where_flag = ""
    if _coluna_existe(cursor, TABELA_DESTINO, 'Flag'):
        where_flag = " AND a.Flag = 1"

    _executar(cursor, f"""
        INSERT INTO dbo.[Estoque_DePara] (est_cd, est_ds)
        SELECT DISTINCT
            RTRIM(LTRIM(a.ESTOQUE_CODIGO)),
            RTRIM(LTRIM(a.ESTOQUE_CODIGO))
        FROM dbo.{dest} a
        WHERE RTRIM(LTRIM(ISNULL(a.ESTOQUE_CODIGO, ''))) <> ''
          {where_flag}
          AND NOT EXISTS (
              SELECT 1
              FROM dbo.[Estoque_DePara] b
              WHERE RTRIM(LTRIM(ISNULL(b.est_cd, ''))) = RTRIM(LTRIM(a.ESTOQUE_CODIGO))
          )
    """)

    if _coluna_existe(cursor, TABELA_DESTINO, 'LOCALIZACAO'):
        _executar(cursor, f"""
            UPDATE b
            SET b.est_ds = RTRIM(LTRIM(b.est_cd))
            FROM dbo.[Estoque_DePara] b
            WHERE EXISTS (
                SELECT 1
                FROM dbo.{dest} a
                WHERE RTRIM(LTRIM(ISNULL(a.ESTOQUE_CODIGO, ''))) = RTRIM(LTRIM(ISNULL(b.est_cd, '')))
                  AND RTRIM(LTRIM(ISNULL(a.ESTOQUE_CODIGO, ''))) <> ''
                  AND RTRIM(LTRIM(ISNULL(a.LOCALIZACAO, ''))) = RTRIM(LTRIM(ISNULL(b.est_ds, '')))
                  AND RTRIM(LTRIM(ISNULL(a.LOCALIZACAO, ''))) <> RTRIM(LTRIM(ISNULL(a.ESTOQUE_CODIGO, '')))
                  {where_flag}
            )
        """)
    logger.info("Estoque_DePara ajustado para ESTOQUE_CODIGO")


def importar_produto_estoque_para_base(df, banco_gx, banco_wf=None):
    """Importa DataFrame validado para DadosGX (ProdutoEstoque_MG)."""
    from db.connection import conectar_segunda_base

    banco_gx = _validar_identificador_sql(banco_gx.strip())
    if not banco_wf:
        return False, "BancoHomo (BancoWF) é obrigatório para De/Para de Estoque.", {}

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

        from utils.importacao_depara_procedures import executar_depara_pos_importacao
        resumo_depara = executar_depara_pos_importacao(cursor, TIPO_LAYOUT, banco_gx, banco_wf)
        corrigir_depara_estoque_codigo(cursor)

        conn.commit()
        resumo = obter_resumo_importacao(cursor, banco_gx, TABELA_DESTINO)
        try:
            soma_estoque = resumir_soma_estoque(cursor)
        except Exception:
            logger.exception("Falha ao totalizar QUANTIDADE e PRECO_MEDIO por estoque")
            soma_estoque = []
        resumo['inseridos_staging'] = total_inserido
        resumo['soma_estoque'] = soma_estoque
        resumo['procedure'] = _cfg.get('procedure', 'up_01_Extrai_ProdutoEstoque_gx')
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
        logger.exception("Erro na importação ProdutoEstoque")
        return False, f"Erro na importação: {e}", {}
    finally:
        cursor.close()
        conn.close()
