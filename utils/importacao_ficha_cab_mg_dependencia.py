"""
Validação de dependência: Fseg_Prd/Srv exigem CHASSI em Ficha_Cab_MG.

O layout 13 Fseg_Cab deve ser importado antes (gera Ficha_Cab_MG).
"""
from logger import logger
from utils.importacao_forn_cli import (
    _executar,
    _quote_col,
    _resolver_coluna,
    _tabela_existe,
    _validar_identificador_sql,
)
from utils.importacao_veiculo_mg_dependencia import normalizar_chassi_chave
from utils.layout_validation import (
    _ordenar_colunas_layout,
    normalizar_texto_campo,
)

TABELA_FICHA_CAB_MG = 'Ficha_Cab_MG'
LAYOUTS_DEPENDEM_FICHA_CAB_MG = frozenset({
    'fseg_prd',
    'fseg_srv',
})

MAX_ERROS_DEPENDENCIA_LISTADOS = 500


def layout_depende_ficha_cab_mg(nome_layout, descricao=None, colunas=None):
    from utils.importacao_fseg_prd import layout_eh_fseg_prd
    if layout_eh_fseg_prd(nome_layout, descricao, colunas):
        return True
    try:
        from utils.importacao_fseg_srv import layout_eh_fseg_srv
        return layout_eh_fseg_srv(nome_layout, descricao, colunas)
    except ImportError:
        return False


def carregar_chassis_ficha_cab_mg(cursor):
    if not _tabela_existe(cursor, TABELA_FICHA_CAB_MG):
        return None

    col_chassi = _resolver_coluna(cursor, TABELA_FICHA_CAB_MG, 'CHASSI')
    if not col_chassi:
        return set()

    chassis = set()
    _executar(cursor, f"SELECT {_quote_col(col_chassi)} FROM dbo.[{TABELA_FICHA_CAB_MG}]")
    for row in cursor.fetchall():
        chave = normalizar_chassi_chave(row[0])
        if chave:
            chassis.add(chave)
    return chassis


def _indice_chassi_layout(layout_colunas):
    colunas = _ordenar_colunas_layout(layout_colunas)
    for idx, col in enumerate(colunas):
        if (col.get('Descricao') or '').strip().upper() == 'CHASSI':
            return idx, (col.get('Descricao') or 'CHASSI').strip()
    return None, 'CHASSI'


def validar_linhas_dependem_ficha_cab_mg(linhas_campos, layout_colunas, chassis_cab, ficha_cab_existe=True):
    erros = []
    idx_chassi, rotulo = _indice_chassi_layout(layout_colunas)
    if idx_chassi is None:
        return [{
            'Linha': 1,
            'Coluna': '(CHASSI)',
            'Erro': 'Coluna CHASSI não encontrada no layout para validar Ficha_Cab_MG.',
        }]

    if not ficha_cab_existe or chassis_cab is None:
        return [{
            'Linha': 1,
            'Coluna': '(Ficha_Cab_MG)',
            'Erro': (
                'Tabela Ficha_Cab_MG não existe no banco do projeto. '
                'Importe primeiro o layout 13 Fseg_Cab.'
            ),
        }]

    if len(chassis_cab) == 0:
        return [{
            'Linha': 1,
            'Coluna': '(Ficha_Cab_MG)',
            'Erro': (
                'Ficha_Cab_MG está vazia. Importe primeiro o layout 13 Fseg_Cab.'
            ),
        }]

    total_faltantes = 0
    for linha_idx, campos in enumerate(linhas_campos):
        bruto = campos[idx_chassi] if len(campos) > idx_chassi else ''
        chave = normalizar_chassi_chave(bruto)
        if not chave:
            continue
        if chave not in chassis_cab:
            total_faltantes += 1
            if len(erros) >= MAX_ERROS_DEPENDENCIA_LISTADOS:
                continue
            lido = normalizar_texto_campo(bruto) or '(vazio)'
            erros.append({
                'Linha': linha_idx + 1,
                'Coluna': rotulo,
                'Erro': (
                    f"CHASSI '{lido}' não encontrado em Ficha_Cab_MG. "
                    'Importe o layout 13 Fseg_Cab antes deste layout.'
                ),
            })

    if total_faltantes > len(erros):
        extras = total_faltantes - len(erros)
        erros.append({
            'Linha': 0,
            'Coluna': '(Ficha_Cab_MG)',
            'Erro': (
                f"Mais {extras} linha(s) com CHASSI não cadastrado em Ficha_Cab_MG "
                f"(total: {total_faltantes}; exibindo até {MAX_ERROS_DEPENDENCIA_LISTADOS})."
            ),
        })

    return erros


def validar_dependencia_ficha_cab_mg(linhas_campos, layout_colunas, layout_nome, layout_descricao=None, banco_gx=None):
    if not layout_depende_ficha_cab_mg(layout_nome, layout_descricao, layout_colunas):
        return []
    if not banco_gx:
        logger.info('Dependência Ficha_Cab_MG: banco não informado — validação na importação.')
        return []

    from db.connection import conectar_segunda_base

    banco_gx = _validar_identificador_sql(banco_gx.strip())
    conn = conectar_segunda_base(banco_gx)
    if not conn:
        return [{
            'Linha': 1,
            'Coluna': '(conexão)',
            'Erro': f'Não foi possível conectar ao banco {banco_gx} para validar Ficha_Cab_MG.',
        }]

    cursor = conn.cursor()
    try:
        existe = _tabela_existe(cursor, TABELA_FICHA_CAB_MG)
        chassis = carregar_chassis_ficha_cab_mg(cursor) if existe else None
        return validar_linhas_dependem_ficha_cab_mg(
            linhas_campos, layout_colunas, chassis, ficha_cab_existe=existe,
        )
    finally:
        cursor.close()
        conn.close()


def validar_dataframe_depende_ficha_cab_mg(cursor, df, layout_nome=None, layout_descricao=None, colunas_layout=None):
    if not layout_depende_ficha_cab_mg(layout_nome, layout_descricao, colunas_layout):
        return

    existe = _tabela_existe(cursor, TABELA_FICHA_CAB_MG)
    chassis = carregar_chassis_ficha_cab_mg(cursor) if existe else None

    linhas = []
    col_chassi = 'CHASSI'
    if df is not None and not df.empty:
        if col_chassi not in df.columns:
            for c in df.columns:
                if str(c).strip().upper() == 'CHASSI':
                    col_chassi = c
                    break
        if col_chassi in df.columns:
            linhas = ([str(v or '')] for v in df[col_chassi])

    erros = validar_linhas_dependem_ficha_cab_mg(
        linhas, [{'Descricao': 'CHASSI', 'Posicao': 1}], chassis, ficha_cab_existe=existe,
    )
    if erros:
        logger.warning(
            "Dependência Ficha_Cab_MG: %s linha(s) com CHASSI não cadastrado (ex.: %s) — importação segue (Flag=0)",
            len(erros), erros[0]['Erro'],
        )
        return

    logger.info("Dependência Ficha_Cab_MG: todos os CHASSI encontrados em %s", TABELA_FICHA_CAB_MG)


def validar_staging_depende_ficha_cab_mg(cursor, tabela_staging):
    existe = _tabela_existe(cursor, TABELA_FICHA_CAB_MG)
    chassis = carregar_chassis_ficha_cab_mg(cursor) if existe else None

    if not existe or chassis is None:
        raise RuntimeError(
            f"Tabela {TABELA_FICHA_CAB_MG} não existe. Importe primeiro o layout 13 Fseg_Cab."
        )
    if len(chassis) == 0:
        raise RuntimeError(
            f"{TABELA_FICHA_CAB_MG} está vazia. Importe primeiro o layout 13 Fseg_Cab."
        )

    col_chassi = _resolver_coluna(cursor, tabela_staging, 'CHASSI')
    if not col_chassi:
        raise RuntimeError(f"Coluna CHASSI não encontrada em {tabela_staging}.")

    _executar(cursor, f"SELECT {_quote_col(col_chassi)} FROM dbo.[{tabela_staging}]")
    faltantes = 0
    for row in cursor.fetchall():
        chave = normalizar_chassi_chave(row[0])
        if chave and chave not in chassis:
            faltantes += 1

    if faltantes:
        # Não aborta a carga: linhas órfãs seguem e tendem a Flag=0 / Ocorrencia.
        logger.warning(
            "%s registro(s) em %s com CHASSI ausente em %s — importação segue (Flag=0)",
            faltantes, tabela_staging, TABELA_FICHA_CAB_MG,
        )
        return

    logger.info("Staging %s validada contra %s", tabela_staging, TABELA_FICHA_CAB_MG)
