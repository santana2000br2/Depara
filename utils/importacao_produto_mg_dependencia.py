"""
Validação de dependência: layouts secundários exigem PRODUTO_REFERENCIA em Produto_MG.

O layout 7 Produto deve ser importado antes (gera Produto_MG).
Usado por: ProdutoEstoque, ProdLocacao, Fseg_Prd.
"""
import re

from logger import logger
from utils.importacao_forn_cli import (
    _executar,
    _quote_col,
    _resolver_coluna,
    _tabela_existe,
    _validar_identificador_sql,
)
from utils.importacao_produto_estoque import layout_eh_produto_estoque
from utils.layout_validation import (
    _ordenar_colunas_layout,
    normalizar_texto_campo,
)

TABELA_PRODUTO_MG = 'Produto_MG'
TIPO_LAYOUT_DEPENDENTE = 'produto_estoque'
LAYOUTS_DEPENDEM_PRODUTO_MG = frozenset({
    TIPO_LAYOUT_DEPENDENTE,
    'prod_locacao',
    'fseg_prd',
})


def layout_depende_produto_mg(nome_layout, descricao=None, colunas=None):
    from utils.importacao_prod_locacao import layout_eh_prod_locacao
    from utils.importacao_fseg_prd import layout_eh_fseg_prd
    return (
        layout_eh_produto_estoque(nome_layout, descricao, colunas)
        or layout_eh_prod_locacao(nome_layout, descricao, colunas)
        or layout_eh_fseg_prd(nome_layout, descricao, colunas)
    )


def _normalizar_referencia(valor):
    return normalizar_texto_campo(valor).upper()


def _normalizar_cnpj(valor):
    return re.sub(r'\D', '', normalizar_texto_campo(valor))


def _chave_produto_mg(referencia, cnpj_empresa):
    ref = _normalizar_referencia(referencia)
    cnpj = _normalizar_cnpj(cnpj_empresa)
    if not ref or not cnpj:
        return None
    return ref, cnpj


def carregar_referencias_produto_mg(cursor, somente_flag_1=True):
    """
    Carrega chaves (PRODUTO_REFERENCIA, CNPJ_EMPRESA) de Produto_MG.
    """
    if not _tabela_existe(cursor, TABELA_PRODUTO_MG):
        return None

    col_ref = _resolver_coluna(cursor, TABELA_PRODUTO_MG, 'PRODUTO_REFERENCIA')
    col_ref_aj = _resolver_coluna(cursor, TABELA_PRODUTO_MG, 'PRODUTO_REFERENCIA_Ajustado')
    col_cnpj = _resolver_coluna(cursor, TABELA_PRODUTO_MG, 'CNPJ_EMPRESA')
    col_flag = _resolver_coluna(cursor, TABELA_PRODUTO_MG, 'Flag', 'FLAG')

    if not col_cnpj or (not col_ref and not col_ref_aj):
        return set()

    cols = []
    if col_ref:
        cols.append(_quote_col(col_ref))
    if col_ref_aj:
        cols.append(_quote_col(col_ref_aj))
    cols.append(_quote_col(col_cnpj))

    where_flag = ''
    if somente_flag_1 and col_flag:
        where_flag = f" WHERE {_quote_col(col_flag)} = 1"

    chaves = set()
    _executar(cursor, f"SELECT {', '.join(cols)} FROM dbo.[{TABELA_PRODUTO_MG}]{where_flag}")
    for row in cursor.fetchall():
        refs = []
        cnpj = ''
        if col_ref and col_ref_aj:
            refs = [row[0], row[1]]
            cnpj = row[2] if len(row) > 2 else ''
        elif col_ref:
            refs = [row[0]]
            cnpj = row[1] if len(row) > 1 else ''
        elif col_ref_aj:
            refs = [row[0]]
            cnpj = row[1] if len(row) > 1 else ''

        for ref in refs:
            chave = _chave_produto_mg(ref, cnpj)
            if chave:
                chaves.add(chave)

    return chaves


def _mensagem_erro_dependencia_produto_mg(chaves_produto_mg, referencia, cnpj_empresa):
    """
    Mensagem objetiva: prioriza crítica de referência quando ela não existe em Produto_MG.
    """
    lido_ref = normalizar_texto_campo(referencia) or '(vazio)'
    lido_cnpj = normalizar_texto_campo(cnpj_empresa) or '(vazio)'
    ref = _normalizar_referencia(referencia)
    cnpj = _normalizar_cnpj(cnpj_empresa)
    refs_mg = {k[0] for k in chaves_produto_mg}
    cnpjs_mg = {k[1] for k in chaves_produto_mg}

    sufixo = ' Importe o layout 7 Produto antes.'

    if ref not in refs_mg:
        return 'PRODUTO_REFERENCIA', (
            f"PRODUTO_REFERENCIA '{lido_ref}' não está cadastrada em Produto_MG (Flag=1)."
            f"{sufixo}"
        )

    if cnpj not in cnpjs_mg:
        return 'CNPJ_EMPRESA', (
            f"CNPJ_EMPRESA '{lido_cnpj}' não está cadastrado em Produto_MG (Flag=1)."
            f"{sufixo}"
        )

    return 'PRODUTO_REFERENCIA', (
        f"PRODUTO_REFERENCIA '{lido_ref}' não está cadastrada para a empresa "
        f"CNPJ_EMPRESA '{lido_cnpj}' em Produto_MG (Flag=1)."
        f"{sufixo}"
    )


def _indices_produto_estoque(layout_colunas):
    colunas = _ordenar_colunas_layout(layout_colunas)
    indices = {}
    for idx, col in enumerate(colunas):
        desc = (col.get('Descricao') or '').strip().upper()
        if desc in ('PRODUTO_REFERENCIA', 'CNPJ_EMPRESA'):
            indices[desc] = idx
    return indices, colunas


MAX_ERROS_DEPENDENCIA_LISTADOS = 500


def validar_linhas_dependem_produto_mg(linhas_campos, layout_colunas, chaves_produto_mg, produto_mg_existe=True):
    erros = []
    indices, colunas = _indices_produto_estoque(layout_colunas)
    idx_ref = indices.get('PRODUTO_REFERENCIA', 1)
    idx_cnpj = indices.get('CNPJ_EMPRESA', 2)
    rotulo_ref = 'PRODUTO_REFERENCIA'
    rotulo_cnpj = 'CNPJ_EMPRESA'
    if idx_ref < len(colunas):
        rotulo_ref = (colunas[idx_ref].get('Descricao') or rotulo_ref).strip()
    if idx_cnpj < len(colunas):
        rotulo_cnpj = (colunas[idx_cnpj].get('Descricao') or rotulo_cnpj).strip()

    if not produto_mg_existe or chaves_produto_mg is None:
        erros.append({
            'Linha': 1,
            'Coluna': '(Produto_MG)',
            'Erro': (
                'Tabela Produto_MG não existe no banco do projeto. '
                'Importe primeiro o layout 7 Produto.'
            ),
        })
        return erros

    if len(chaves_produto_mg) == 0:
        erros.append({
            'Linha': 1,
            'Coluna': '(Produto_MG)',
            'Erro': (
                'Produto_MG está vazia ou sem registros aptos (Flag=1). '
                'Importe primeiro o layout 7 Produto.'
            ),
        })
        return erros

    total_faltantes = 0
    for linha_idx, campos in enumerate(linhas_campos):
        ref = campos[idx_ref] if len(campos) > idx_ref else ''
        cnpj = campos[idx_cnpj] if len(campos) > idx_cnpj else ''
        chave = _chave_produto_mg(ref, cnpj)
        if not chave:
            continue
        if chave not in chaves_produto_mg:
            total_faltantes += 1
            if len(erros) >= MAX_ERROS_DEPENDENCIA_LISTADOS:
                continue
            coluna_erro, mensagem = _mensagem_erro_dependencia_produto_mg(
                chaves_produto_mg, ref, cnpj,
            )
            rotulo = rotulo_ref if coluna_erro == 'PRODUTO_REFERENCIA' else rotulo_cnpj
            erros.append({
                'Linha': linha_idx + 1,
                'Coluna': rotulo,
                'Erro': mensagem,
            })

    if total_faltantes > len(erros):
        extras = total_faltantes - len(erros)
        erros.append({
            'Linha': 0,
            'Coluna': '(Produto_MG)',
            'Erro': (
                f"Mais {extras} linha(s) com produto não cadastrado em Produto_MG "
                f"(total: {total_faltantes}; exibindo até {MAX_ERROS_DEPENDENCIA_LISTADOS})."
            ),
        })

    return erros


def validar_dependencia_produto_mg(linhas_campos, layout_colunas, layout_nome, layout_descricao=None, banco_gx=None):
    if not layout_depende_produto_mg(layout_nome, layout_descricao, layout_colunas):
        return []

    if not banco_gx:
        logger.info('Dependência Produto_MG: banco não informado — validação na importação.')
        return []

    from db.connection import conectar_segunda_base

    banco_gx = _validar_identificador_sql(banco_gx.strip())
    conn = conectar_segunda_base(banco_gx)
    if not conn:
        return [{
            'Linha': 1,
            'Coluna': '(conexão)',
            'Erro': f'Não foi possível conectar ao banco {banco_gx} para validar Produto_MG.',
        }]

    cursor = conn.cursor()
    try:
        existe = _tabela_existe(cursor, TABELA_PRODUTO_MG)
        chaves = carregar_referencias_produto_mg(cursor) if existe else None
        return validar_linhas_dependem_produto_mg(
            linhas_campos, layout_colunas, chaves, produto_mg_existe=existe,
        )
    finally:
        cursor.close()
        conn.close()


def validar_dataframe_depende_produto_mg(cursor, df, layout_nome=None, layout_descricao=None, colunas_layout=None):
    if not layout_depende_produto_mg(layout_nome, layout_descricao, colunas_layout):
        return

    existe = _tabela_existe(cursor, TABELA_PRODUTO_MG)
    chaves = carregar_referencias_produto_mg(cursor) if existe else None

    colunas = _ordenar_colunas_layout(colunas_layout or [])
    linhas = []
    if df is not None and not df.empty:
        mapa_cols = {str(c).strip().upper(): c for c in df.columns}
        for _, row in df.iterrows():
            campos = []
            for col in colunas:
                desc = (col.get('Descricao') or '').strip().upper()
                nome_col = mapa_cols.get(desc)
                campos.append(str(row.get(nome_col, '') or '') if nome_col else '')
            linhas.append(campos)

    erros = validar_linhas_dependem_produto_mg(linhas, colunas, chaves, produto_mg_existe=existe)
    if erros:
        amostra = erros[0]['Erro']
        total = len(erros)
        logger.warning(
            "Dependência Produto_MG: %s linha(s) com produto não cadastrado (ex.: %s) — importação segue (Flag=0)",
            total, amostra,
        )
        return

    logger.info("Dependência Produto_MG: referências validadas em %s", TABELA_PRODUTO_MG)


def validar_staging_depende_produto_mg(cursor, tabela_staging):
    existe = _tabela_existe(cursor, TABELA_PRODUTO_MG)
    chaves = carregar_referencias_produto_mg(cursor) if existe else None

    if not existe or chaves is None:
        raise RuntimeError(
            f"Tabela {TABELA_PRODUTO_MG} não existe. Importe primeiro o layout 7 Produto."
        )
    if len(chaves) == 0:
        raise RuntimeError(
            f"{TABELA_PRODUTO_MG} está vazia ou sem Flag=1. Importe primeiro o layout 7 Produto."
        )

    col_ref = _resolver_coluna(cursor, tabela_staging, 'PRODUTO_REFERENCIA')
    col_cnpj = _resolver_coluna(cursor, tabela_staging, 'CNPJ_EMPRESA')
    if not col_ref or not col_cnpj:
        raise RuntimeError(
            f"Colunas PRODUTO_REFERENCIA/CNPJ_EMPRESA não encontradas em {tabela_staging}."
        )

    _executar(
        cursor,
        f"SELECT {_quote_col(col_ref)}, {_quote_col(col_cnpj)} FROM dbo.[{tabela_staging}]",
    )
    faltantes = 0
    for row in cursor.fetchall():
        chave = _chave_produto_mg(row[0], row[1])
        if chave and chave not in chaves:
            faltantes += 1

    if faltantes:
        # Não aborta a carga: linhas órfãs seguem e tendem a Flag=0 / Ocorrencia.
        logger.warning(
            "%s registro(s) em %s com PRODUTO_REFERENCIA ausente em %s — importação segue (Flag=0)",
            faltantes, tabela_staging, TABELA_PRODUTO_MG,
        )
        return

    logger.info("Staging %s validada contra %s", tabela_staging, TABELA_PRODUTO_MG)
