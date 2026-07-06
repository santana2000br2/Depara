"""
Gera tabelas De/Para a partir de Pessoa_MG (Segmento, Escolaridade, Profissão, Estado Civil).
"""
from logger import logger
from utils.importacao_forn_cli import (
    TABELA_DESTINO,
    _alias_col,
    _executar,
    _quote_col,
    _quote_db,
    _quote_table,
    _resolver_coluna,
    _tabela_existe,
    _validar_identificador_sql,
)

SEM_DEPARA = 'S/DePara'

SEGMENTOS_PESSOA = (
    ('SEGMENTO_OFICINA', 'SEGMENTO_OFICINA'),
    ('SEGMENTO_BALCAO', 'SEGMENTO_BALCAO'),
    ('SEGMENTO_VENDAS', 'SEGMENTO_VENDAS'),
)


def _refs_pessoa_mg(cursor):
    return {
        'flag': _resolver_coluna(cursor, TABELA_DESTINO, 'Flag', 'FLAG'),
        'tipo': _resolver_coluna(cursor, TABELA_DESTINO, 'TIPO', 'Tipo'),
        'escola_cd': _resolver_coluna(cursor, TABELA_DESTINO, 'ESCOLARIDADE_CODIGO'),
        'escola_ds': _resolver_coluna(cursor, TABELA_DESTINO, 'ESCOLARIDADE_DESCRICAO'),
        'prof_cd': _resolver_coluna(cursor, TABELA_DESTINO, 'PROFISSAO_CODIGO'),
        'prof_ds': _resolver_coluna(cursor, TABELA_DESTINO, 'PROFISSAO_DESCRICAO'),
        'estcivil_cd': _resolver_coluna(cursor, TABELA_DESTINO, 'ESTADO_CIVIL_CODIGO'),
        'estcivil_ds': _resolver_coluna(cursor, TABELA_DESTINO, 'ESTADO_CIVIL_DESCRICAO'),
    }


def _gerar_segmento_mercado_depara(cursor, gx, wf, dest, refs):
    """INSERT novos segmentos (sem truncar) + UPDATE apenas S/DePara com base WF."""
    tabela = 'SegmentoMercado_DePara'
    resumo = {'inseridos': 0, 'atualizados_wf': 0}

    col_flag = refs.get('flag')
    if not col_flag:
        logger.warning("Flag não encontrada em Pessoa_MG — SegmentoMercado_DePara ignorado.")
        return resumo

    flag = _alias_col('a', col_flag)
    partes = []

    for col_layout, _ in SEGMENTOS_PESSOA:
        col_seg = _resolver_coluna(cursor, TABELA_DESTINO, col_layout)
        if not col_seg:
            continue
        seg = _alias_col('a', col_seg)
        partes.append(f"""
            SELECT DISTINCT
                '' AS segm_cd,
                ISNULL({seg}, '') AS segm_ds,
                '{SEM_DEPARA}' AS SegmentoMercado_Codigo,
                '{SEM_DEPARA}' AS SegmentoMercado_Descricao
            FROM {gx}.dbo.{dest} a
            WHERE {flag} = 1
              AND NOT EXISTS (
                  SELECT 1 FROM {gx}.dbo.[{tabela}] c
                  WHERE ISNULL({seg}, '') = ISNULL(c.segm_ds, '')
              )
        """)

    if not partes:
        return resumo

    sql_insert = f"""
        INSERT INTO {gx}.dbo.[{tabela}]
            (segm_cd, segm_ds, SegmentoMercado_Codigo, SegmentoMercado_Descricao)
        {' UNION '.join(partes)}
    """
    _executar(cursor, sql_insert)
    resumo['inseridos'] = cursor.rowcount if cursor.rowcount >= 0 else 0

    _executar(cursor, f"""
        UPDATE a
        SET a.SegmentoMercado_Codigo = b.SegmentoMercado_Codigo,
            a.SegmentoMercado_Descricao = b.SegmentoMercado_Descricao
        FROM {gx}.dbo.[{tabela}] a
        INNER JOIN {wf}.dbo.[SegmentoMercado] b
            ON b.SegmentoMercado_Descricao = a.segm_ds
        WHERE a.SegmentoMercado_Codigo = '{SEM_DEPARA}'
    """)
    resumo['atualizados_wf'] = cursor.rowcount if cursor.rowcount >= 0 else 0
    return resumo


def _gerar_escolaridade_depara(cursor, gx, wf, dest, refs):
    tabela = 'Escolaridade_DePara'
    resumo = {'inseridos': 0, 'atualizados_wf': 0}

    col_flag = refs.get('flag')
    col_tipo = refs.get('tipo')
    col_cd = refs.get('escola_cd')
    col_ds = refs.get('escola_ds')
    if not all([col_flag, col_tipo, col_cd]):
        logger.warning("Colunas insuficientes para Escolaridade_DePara.")
        return resumo

    flag = _alias_col('a', col_flag)
    tipo = _alias_col('a', col_tipo)
    cd = _alias_col('a', col_cd)
    ds = _alias_col('a', col_ds) if col_ds else "''"

    _executar(cursor, f"""
        INSERT INTO {gx}.dbo.[{tabela}]
            (escola_cd, escola_ds, Escolaridade_Codigo, Escolaridade_Descricao)
        SELECT DISTINCT
            ISNULL({cd}, '') AS escola_cd,
            ISNULL({ds}, '') AS escola_ds,
            '{SEM_DEPARA}' AS Escolaridade_Codigo,
            '{SEM_DEPARA}' AS Escolaridade_Descricao
        FROM {gx}.dbo.{dest} a
        WHERE {tipo} = 'F'
          AND {cd} IS NOT NULL
          AND {flag} = 1
          AND NOT EXISTS (
              SELECT 1 FROM {gx}.dbo.[{tabela}] b
              WHERE ISNULL({cd}, '') = ISNULL(b.escola_cd, '')
          )
    """)
    resumo['inseridos'] = cursor.rowcount if cursor.rowcount >= 0 else 0

    _executar(cursor, f"""
        UPDATE a
        SET a.Escolaridade_Codigo = b.Escolaridade_Codigo,
            a.Escolaridade_Descricao = b.Escolaridade_Descricao
        FROM {gx}.dbo.[{tabela}] a
        INNER JOIN {wf}.dbo.[Escolaridade] b
            ON b.Escolaridade_Descricao = a.escola_ds
        WHERE a.Escolaridade_Codigo = '{SEM_DEPARA}'
    """)
    resumo['atualizados_wf'] = cursor.rowcount if cursor.rowcount >= 0 else 0
    return resumo


def _gerar_profissao_depara(cursor, gx, wf, dest, refs):
    tabela = 'Profissao_DePara'
    resumo = {'inseridos': 0, 'atualizados_wf': 0}

    col_flag = refs.get('flag')
    col_tipo = refs.get('tipo')
    col_cd = refs.get('prof_cd')
    col_ds = refs.get('prof_ds')
    if not all([col_flag, col_tipo, col_cd]):
        logger.warning("Colunas insuficientes para Profissao_DePara.")
        return resumo

    flag = _alias_col('a', col_flag)
    tipo = _alias_col('a', col_tipo)
    cd = _alias_col('a', col_cd)
    ds = _alias_col('a', col_ds) if col_ds else "''"

    _executar(cursor, f"""
        INSERT INTO {gx}.dbo.[{tabela}]
            (prof_cd, prof_ds, Profissao_Codigo, Profissao_Descricao)
        SELECT DISTINCT
            ISNULL({cd}, '') AS prof_cd,
            ISNULL({ds}, '') AS prof_ds,
            '{SEM_DEPARA}' AS Profissao_Codigo,
            '{SEM_DEPARA}' AS Profissao_Descricao
        FROM {gx}.dbo.{dest} a
        WHERE {tipo} = 'F'
          AND {cd} IS NOT NULL
          AND {flag} = 1
          AND NOT EXISTS (
              SELECT 1 FROM {gx}.dbo.[{tabela}] b
              WHERE ISNULL({cd}, '') = ISNULL(b.prof_cd, '')
          )
    """)
    resumo['inseridos'] = cursor.rowcount if cursor.rowcount >= 0 else 0

    _executar(cursor, f"""
        UPDATE a
        SET a.Profissao_Codigo = b.Profissao_Codigo,
            a.Profissao_Descricao = b.Profissao_Descricao
        FROM {gx}.dbo.[{tabela}] a
        INNER JOIN {wf}.dbo.[Profissao] b
            ON b.Profissao_Descricao = a.prof_ds COLLATE Latin1_General_CI_AI
        WHERE a.Profissao_Codigo = '{SEM_DEPARA}'
    """)
    resumo['atualizados_wf'] = cursor.rowcount if cursor.rowcount >= 0 else 0
    return resumo


def _gerar_estadocivil_depara(cursor, gx, wf, dest, refs):
    tabela = 'EstadoCivil_DePara'
    resumo = {'inseridos': 0, 'atualizados_wf': 0}

    col_flag = refs.get('flag')
    col_tipo = refs.get('tipo')
    col_cd = refs.get('estcivil_cd')
    col_ds = refs.get('estcivil_ds')
    if not all([col_flag, col_tipo, col_cd]):
        logger.warning("Colunas insuficientes para EstadoCivil_DePara.")
        return resumo

    flag = _alias_col('a', col_flag)
    tipo = _alias_col('a', col_tipo)
    cd = _alias_col('a', col_cd)
    ds = _alias_col('a', col_ds) if col_ds else "''"

    _executar(cursor, f"""
        INSERT INTO {gx}.dbo.[{tabela}]
            (estcivil_cd, estcivil_ds, EstadoCivil_Codigo, EstadoCivil_Descricao)
        SELECT DISTINCT
            ISNULL({cd}, '') AS estcivil_cd,
            ISNULL({ds}, '') AS estcivil_ds,
            '{SEM_DEPARA}' AS EstadoCivil_Codigo,
            '{SEM_DEPARA}' AS EstadoCivil_Descricao
        FROM {gx}.dbo.{dest} a
        WHERE {tipo} = 'F'
          AND {cd} IS NOT NULL
          AND {flag} = 1
          AND NOT EXISTS (
              SELECT 1 FROM {gx}.dbo.[{tabela}] b
              WHERE ISNULL({cd}, '') = ISNULL(b.estcivil_cd, '')
          )
    """)
    resumo['inseridos'] = cursor.rowcount if cursor.rowcount >= 0 else 0

    _executar(cursor, f"""
        UPDATE a
        SET a.EstadoCivil_Codigo = b.EstadoCivil_Codigo,
            a.EstadoCivil_Descricao = b.EstadoCivil_Descricao
        FROM {gx}.dbo.[{tabela}] a
        INNER JOIN {wf}.dbo.[EstadoCivil] b
            ON REPLACE(b.EstadoCivil_Descricao, '(A)', '')
                = a.estcivil_ds COLLATE Latin1_General_CI_AI
        WHERE a.EstadoCivil_Codigo = '{SEM_DEPARA}'
    """)
    resumo['atualizados_wf'] = cursor.rowcount if cursor.rowcount >= 0 else 0
    return resumo


def gerar_depara_pessoa_mg(cursor, banco_gx, banco_wf):
    """Gera De/Para via procedures legadas (up_01 a up_04)."""
    from utils.importacao_depara_procedures import executar_depara_pos_importacao
    return executar_depara_pos_importacao(cursor, 'forn_cli', banco_gx, banco_wf)


def gerar_depara_pessoa_mg_standalone(banco_gx, banco_wf):
    """Abre conexão, gera De/Para e faz commit. Retorna (sucesso, mensagem, resumo)."""
    from db.connection import conectar_segunda_base

    banco_gx = _validar_identificador_sql(banco_gx.strip())
    banco_wf = _validar_identificador_sql(banco_wf.strip())

    conn = conectar_segunda_base(banco_gx)
    if not conn:
        return False, f"Não foi possível conectar ao banco {banco_gx}.", {}

    cursor = conn.cursor()
    try:
        resumo = gerar_depara_pessoa_mg(cursor, banco_gx, banco_wf)
        conn.commit()
        msg = (
            f"De/Para gerado via procedure(s): "
            f"{', '.join(resumo.get('procedures_executadas', [])) or 'up_01–up_04'}. "
            f"{formatar_resumo_depara(resumo)}"
        )
        return True, msg, resumo
    except Exception as e:
        conn.rollback()
        logger.exception("Erro ao gerar De/Para Pessoa_MG")
        return False, f"Erro ao gerar De/Para: {e}", {}
    finally:
        cursor.close()
        conn.close()


def formatar_resumo_depara(resumo):
    if not resumo:
        return ''
    partes = []
    labels = {
        'segmento_mercado': 'Segmento Mercado',
        'escolaridade': 'Escolaridade',
        'profissao': 'Profissão',
        'estado_civil': 'Estado Civil',
        'municipio': 'Município',
        'tipo_logradouro': 'Tipo Logradouro',
        'estado': 'Estado',
        'pais': 'País',
        'banco': 'Banco',
    }
    meta = {'via_procedure', 'procedures_executadas'}
    for chave, label in labels.items():
        if chave in meta or chave not in resumo:
            continue
        item = resumo.get(chave, {})
        if resumo.get('via_procedure'):
            partes.append(
                f"{label}: {item.get('vinculados_wf', 0)} vinculado(s) WF, "
                f"{item.get('pendentes', 0)} S/DePara"
            )
        else:
            partes.append(
                f"{label}: {item.get('inseridos', 0)} novo(s), "
                f"{item.get('atualizados_wf', 0)} vinculado(s) ao WF"
            )
    if resumo.get('procedures_executadas'):
        partes.append(f"({len(resumo['procedures_executadas'])} procedure(s))")
    return ' · '.join(partes)
