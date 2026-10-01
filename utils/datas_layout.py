"""
Conversão de datas dos layouts (dd/mm/aaaa) para aaaa-mm-dd.

ISDATE/CAST sem style segue o DATEFORMAT da sessão. Com mdy (inglês),
24/09/1985 é mês 24 e a procedure grava 1900-01-01. Dia <= 12 troca dia e mês.
Style 103 (dd/mm/aaaa) e 23 (aaaa-mm-dd) não dependem do idioma.
"""

from logger import logger

# modo: sentinel → inválida/vazia vira 1900-01-01
#       hoje → vazia vira a data de hoje; inválida vira 1900-01-01
#       preservar → vazia permanece vazia; inválida vira 1900-01-01
DATAS_LAYOUT = {
    'forn_cli': {
        'DT_ANIVER': 'sentinel',
        'DATA_CADASTRO': 'hoje',
        'LIM_CREDITO_VALIDADE': 'sentinel',
    },
    'forn_cli_contato': {
        'DT_ANIVER': 'sentinel',
    },
    'veiculo': {
        'DATA_VENDA': 'sentinel',
    },
    'fseg_cab': {
        'DATA_ABERTURA': 'sentinel',
        'DATA_LIBERACAO': 'sentinel',
    },
    'financeiro': {
        'DATA_EMISSAO': 'preservar',
        'DATA_ENTRADA': 'preservar',
        'DATA_VENCIMENTO': 'preservar',
    },
    'adiantamento': {
        'DATA_MOVIMENTO': 'preservar',
    },
    'movimento_estoque': {
        'DATA_MOVIMENTO': 'sentinel',
    },
}


def sql_valor_data(expr, modo='sentinel'):
    """Expressão SQL que devolve varchar(10) aaaa-mm-dd a partir de expr."""
    bruto = f"LTRIM(RTRIM(CONVERT(varchar(30), {expr}, 23)))"
    token = f"LEFT({bruto}, 10)"
    barra = f"REPLACE(REPLACE({token}, '-', '/'), '.', '/')"
    traco = f"REPLACE({token}, '/', '-')"
    convertido = (
        "COALESCE("
        f"CONVERT(varchar(10), TRY_CONVERT(date, {barra}, 103), 23),"
        f"CONVERT(varchar(10), TRY_CONVERT(date, {traco}, 23), 23),"
        f"CONVERT(varchar(10), TRY_CONVERT(date, {bruto}, 112), 23),"
        f"CASE WHEN LEN({token}) <= 8 "
        f"THEN CONVERT(varchar(10), TRY_CONVERT(date, {barra}, 3), 23) END,"
        "'1900-01-01')"
    )
    vazio = f"{expr} IS NULL OR {bruto} = ''"
    if modo == 'hoje':
        return (
            "CASE "
            f"WHEN {vazio} THEN CONVERT(varchar(10), CAST(GETDATE() AS date), 23) "
            f"ELSE {convertido} END"
        )
    if modo == 'preservar':
        return (
            "CASE "
            f"WHEN {vazio} THEN CONVERT(varchar(30), {expr}) "
            f"ELSE {convertido} END"
        )
    return convertido


def aplicar_datas_layout(cursor, tipo_layout, fonte='staging'):
    """
    Grava datas do layout em aaaa-mm-dd na tabela de destino.

    fonte='staging': lê o texto original (dd/mm/aaaa) e desfaz 1900-01-01
    gerado pela procedure legada.
    fonte='destino': converte a própria tabela (cópia ainda em dd/mm/aaaa),
    antes de CAST/ISDATE dentro da procedure.
    """
    spec = DATAS_LAYOUT.get(tipo_layout)
    if not spec:
        return

    from utils.importacao_forn_cli import (
        _coluna_existe,
        _executar,
        _quote_col,
        _quote_table,
        _resolver_coluna,
        _tabela_existe,
    )
    from utils.importacao_procedures import obter_config_procedure

    cfg = obter_config_procedure(tipo_layout)
    if not cfg:
        return

    staging = cfg['staging']
    destino = cfg['destino']
    if not _tabela_existe(cursor, destino):
        return

    usar_staging = fonte == 'staging' and _tabela_existe(cursor, staging)
    id_dest = _resolver_coluna(cursor, destino, 'IDtabela', 'IDTABELA') if usar_staging else None
    id_stg = _resolver_coluna(cursor, staging, 'IDtabela', 'IDTABELA') if usar_staging else None
    if usar_staging and not (id_dest and id_stg):
        logger.warning(
            "Datas %s: sem IDtabela para cruzar %s e %s — conversão no próprio destino",
            tipo_layout, staging, destino,
        )
        usar_staging = False

    dest_sql = _quote_table(destino)
    sets = []
    for coluna, modo in spec.items():
        if not _coluna_existe(cursor, destino, coluna):
            continue
        if usar_staging and not _coluna_existe(cursor, staging, coluna):
            continue
        origem = f"b.{_quote_col(coluna)}" if usar_staging else f"a.{_quote_col(coluna)}"
        sets.append(f"a.{_quote_col(coluna)} = {sql_valor_data(origem, modo)}")

    if not sets:
        return

    if usar_staging:
        sql = (
            f"UPDATE a SET {', '.join(sets)} "
            f"FROM dbo.{dest_sql} a "
            f"INNER JOIN dbo.{_quote_table(staging)} b "
            f"ON a.{_quote_col(id_dest)} = b.{_quote_col(id_stg)}"
        )
    else:
        sql = f"UPDATE a SET {', '.join(sets)} FROM dbo.{dest_sql} a"

    _executar(cursor, sql)
    logger.info(
        "Datas normalizadas em %s (%s, fonte=%s)",
        destino, ', '.join(spec.keys()), 'staging' if usar_staging else 'destino',
    )
