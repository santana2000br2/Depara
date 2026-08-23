"""
Validação de dependência: Fseg_Cab exige CHASSI cadastrado em Veiculo_MG.

O layout Veiculo deve ser importado antes (gera Veiculo_MG).
"""
import re

from logger import logger
from utils.importacao_forn_cli import (
    _executar,
    _normalizar_nome_layout,
    _quote_col,
    _resolver_coluna,
    _tabela_existe,
    _validar_identificador_sql,
)
from utils.layout_validation import (
    _ordenar_colunas_layout,
    normalizar_texto_campo,
)

TABELA_VEICULO_MG = 'Veiculo_MG'
LAYOUTS_DEPENDEM_VEICULO_MG = frozenset({
    'fseg_cab',
})

MAX_ERROS_DEPENDENCIA_LISTADOS = 500


def layout_depende_veiculo_mg(nome_layout, descricao=None, colunas=None):
    from utils.importacao_fseg_cab import layout_eh_fseg_cab
    return layout_eh_fseg_cab(nome_layout, descricao, colunas)


def normalizar_chassi_chave(valor):
    """Normaliza CHASSI para comparação (letras/números maiúsculos)."""
    texto = normalizar_texto_campo(valor).upper()
    digitos_letras = re.sub(r'[^A-Z0-9]', '', texto)
    return digitos_letras or None


def carregar_chassis_veiculo_mg(cursor, somente_flag_1=True):
    if not _tabela_existe(cursor, TABELA_VEICULO_MG):
        return None

    col_chassi = _resolver_coluna(cursor, TABELA_VEICULO_MG, 'CHASSI')
    col_flag = _resolver_coluna(cursor, TABELA_VEICULO_MG, 'Flag', 'FLAG')
    if not col_chassi:
        return set()

    where_flag = ''
    if somente_flag_1 and col_flag:
        where_flag = f" WHERE {_quote_col(col_flag)} = 1"

    chassis = set()
    _executar(cursor, f"SELECT {_quote_col(col_chassi)} FROM dbo.[{TABELA_VEICULO_MG}]{where_flag}")
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


def validar_linhas_dependem_veiculo_mg(linhas_campos, layout_colunas, chassis_mg, veiculo_mg_existe=True):
    erros = []
    idx_chassi, rotulo = _indice_chassi_layout(layout_colunas)
    if idx_chassi is None:
        return [{
            'Linha': 1,
            'Coluna': '(CHASSI)',
            'Erro': 'Coluna CHASSI não encontrada no layout para validar Veiculo_MG.',
        }]

    if not veiculo_mg_existe or chassis_mg is None:
        erros.append({
            'Linha': 1,
            'Coluna': '(Veiculo_MG)',
            'Erro': (
                'Tabela Veiculo_MG não existe no banco do projeto. '
                'Importe primeiro o layout Veiculo.'
            ),
        })
        return erros

    if len(chassis_mg) == 0:
        erros.append({
            'Linha': 1,
            'Coluna': '(Veiculo_MG)',
            'Erro': (
                'Veiculo_MG está vazia. Importe primeiro o layout Veiculo '
                'com os chassis cadastrados.'
            ),
        })
        return erros

    total_faltantes = 0
    for linha_idx, campos in enumerate(linhas_campos):
        bruto = campos[idx_chassi] if len(campos) > idx_chassi else ''
        chave = normalizar_chassi_chave(bruto)
        if not chave:
            continue
        if chave not in chassis_mg:
            total_faltantes += 1
            if len(erros) >= MAX_ERROS_DEPENDENCIA_LISTADOS:
                continue
            lido = normalizar_texto_campo(bruto) or '(vazio)'
            erros.append({
                'Linha': linha_idx + 1,
                'Coluna': rotulo,
                'Erro': (
                    f"CHASSI '{lido}' não encontrado em Veiculo_MG. "
                    'Importe o cadastro de Veículo antes deste layout.'
                ),
            })

    if total_faltantes > len(erros):
        extras = total_faltantes - len(erros)
        erros.append({
            'Linha': 0,
            'Coluna': '(Veiculo_MG)',
            'Erro': (
                f"Mais {extras} linha(s) com CHASSI não cadastrado em Veiculo_MG "
                f"(total: {total_faltantes}; exibindo até {MAX_ERROS_DEPENDENCIA_LISTADOS})."
            ),
        })

    return erros


def validar_dependencia_veiculo_mg(linhas_campos, layout_colunas, layout_nome, layout_descricao=None, banco_gx=None):
    if not layout_depende_veiculo_mg(layout_nome, layout_descricao, layout_colunas):
        return []
    if not banco_gx:
        logger.info('Dependência Veiculo_MG: banco não informado — validação na importação.')
        return []

    from db.connection import conectar_segunda_base

    banco_gx = _validar_identificador_sql(banco_gx.strip())
    conn = conectar_segunda_base(banco_gx)
    if not conn:
        return [{
            'Linha': 1,
            'Coluna': '(conexão)',
            'Erro': f'Não foi possível conectar ao banco {banco_gx} para validar Veiculo_MG.',
        }]

    cursor = conn.cursor()
    try:
        existe = _tabela_existe(cursor, TABELA_VEICULO_MG)
        chassis = carregar_chassis_veiculo_mg(cursor) if existe else None
        return validar_linhas_dependem_veiculo_mg(
            linhas_campos, layout_colunas, chassis, veiculo_mg_existe=existe,
        )
    finally:
        cursor.close()
        conn.close()


def validar_dataframe_depende_veiculo_mg(cursor, df, layout_nome=None, layout_descricao=None, colunas_layout=None):
    if not layout_depende_veiculo_mg(layout_nome, layout_descricao, colunas_layout):
        return

    existe = _tabela_existe(cursor, TABELA_VEICULO_MG)
    chassis = carregar_chassis_veiculo_mg(cursor) if existe else None

    linhas = []
    col_chassi = 'CHASSI'
    if df is not None and not df.empty:
        if col_chassi not in df.columns:
            for c in df.columns:
                if str(c).strip().upper() == 'CHASSI':
                    col_chassi = c
                    break
        for _, row in df.iterrows():
            linhas.append([str(row.get(col_chassi, '') or '')])

    colunas = colunas_layout or [{'Descricao': 'CHASSI', 'Posicao': 1}]
    # Alinha com validar_linhas: um único campo no índice 0 = CHASSI
    colunas_simples = [{'Descricao': 'CHASSI', 'Posicao': 1}]
    erros = validar_linhas_dependem_veiculo_mg(linhas, colunas_simples, chassis, veiculo_mg_existe=existe)
    if erros:
        amostra = erros[0]['Erro']
        logger.warning(
            "Dependência Veiculo_MG: %s linha(s) com CHASSI não cadastrado (ex.: %s) — importação segue (Flag=0)",
            len(erros), amostra,
        )
        return

    logger.info("Dependência Veiculo_MG: todos os CHASSI encontrados em %s", TABELA_VEICULO_MG)


def validar_staging_depende_veiculo_mg(cursor, tabela_staging):
    existe = _tabela_existe(cursor, TABELA_VEICULO_MG)
    chassis = carregar_chassis_veiculo_mg(cursor) if existe else None

    if not existe or chassis is None:
        raise RuntimeError(
            f"Tabela {TABELA_VEICULO_MG} não existe. Importe primeiro o layout Veiculo."
        )
    if len(chassis) == 0:
        raise RuntimeError(
            f"{TABELA_VEICULO_MG} está vazia. Importe primeiro o layout Veiculo."
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
            faltantes, tabela_staging, TABELA_VEICULO_MG,
        )
        return

    logger.info("Staging %s validada contra %s", tabela_staging, TABELA_VEICULO_MG)
