"""
Importação Forn_cli → Arquivo_Forn_cli_Tratado → Pessoa_MG (banco DadosGX).

Carga Python na staging + procedure legada up_01_Extrai_Pessoa_gx + De/Para.
"""
import re
import pandas as pd
from logger import logger

TABELA_STAGING = 'Arquivo_Forn_cli_Tratado'
TABELA_DESTINO = 'Pessoa_MG'

COLUNAS_EXTRA_PESSOA_MG = [
    ('Flag', 'SMALLINT NULL'),
    ('Pessoa_DocIdentificador', 'varchar(20) null'),
    ('EstadoCivil_CodigoWF', 'smallint NULL'),
    ('Escolaridade_CodigoWF', 'smallint NULL'),
    ('Profissao_CodigoWF', 'smallint NULL'),
    ('Pessoa_PaisOrigemCod', 'smallint NULL'),
    ('Pessoa_SegmentoMercado_Balcao', 'smallint NULL'),
    ('Pessoa_SegmentoMercado_Oficina', 'smallint NULL'),
    ('Pessoa_SegmentoMercado_Vendas', 'smallint NULL'),
    ('PessoaRegraUso_Chave', 'int NULL'),
    ('Pessoa_CodigoMyHonda', 'bigint NULL'),
    ('Pessoa_BloqueiaVendaTituloAtraso', 'smallint NULL'),
    ('Pessoa_BloqueiaEntradaOficina', 'smallint NULL'),
    ('Ocorrencia', 'VARCHAR(500) null'),
]

FORN_CLI_SUB_LAYOUTS = frozenset({
    'forn_cli_endereco',
    'forn_cli_documento',
    'forn_cli_enquadramento',
    'forn_cli_telefone',
    'forn_cli_contato',
})

FORN_CLI_COLUNAS_CHAVE = frozenset({
    'CODIGO_PESSOA', 'NOME', 'TIPO', 'CPF_CNPJ', 'E_MAIL',
})


def _normalizar_nome_layout(texto):
    """Ex.: '1 Forn_cli.txt' → 'forn_cli'; '2 Forn_cli Endereco' → 'forn_cli_endereco'."""
    if not texto:
        return ''
    nome = str(texto).strip().lower()
    nome = re.sub(r'^\d+\s*', '', nome)
    nome = re.sub(r'\.txt$', '', nome)
    nome = re.sub(r'\s+', '_', nome)
    return nome.strip()


def layout_eh_forn_cli(nome_layout, descricao=None, colunas=None):
    """
    Reconhece layout principal Forn_cli.
    Aceita nomes como 'Forn_cli', '1 Forn_cli.txt', 'FORN_CLI - Cadastro'.
    Exclui sub-layouts (Endereco, Documento, etc.).
    """
    for texto in (nome_layout, descricao):
        nome = _normalizar_nome_layout(texto)
        if not nome:
            continue
        if nome in FORN_CLI_SUB_LAYOUTS or any(
            nome.startswith(f'{sub}_') or nome == sub for sub in FORN_CLI_SUB_LAYOUTS
        ):
            continue
        if nome == 'forn_cli':
            return True
        if nome.startswith('forn_cli_'):
            continue
        if 'forn_cli' in nome:
            return True

    if colunas:
        nomes = set()
        for c in colunas:
            if isinstance(c, dict):
                nomes.add(str(c.get('Descricao') or '').strip().upper())
            else:
                nomes.add(str(c).strip().upper())
        nomes.discard('')
        if FORN_CLI_COLUNAS_CHAVE.issubset(nomes):
            return True

    return False


def _validar_identificador_sql(nome):
    if not nome or not re.match(r'^[A-Za-z0-9_]+$', str(nome).strip()):
        raise ValueError(f"Identificador SQL inválido: {nome!r}")
    return str(nome).strip()


def _quote_db(nome):
    return f"[{_validar_identificador_sql(nome)}]"


def _quote_table(nome):
    return f"[{_validar_identificador_sql(nome)}]"


def _quote_col(nome):
    """Colunas do layout podem ter acentos/espaços — só escapa colchetes."""
    col = str(nome or '').strip()
    if not col:
        raise ValueError("Nome de coluna vazio")
    return f"[{col.replace(']', ']]')}]"


def _normalizar_colunas_dataframe(df):
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df


def _tabela_existe(cursor, tabela):
    cursor.execute(
        "SELECT 1 FROM sys.objects WHERE type = 'U' AND name = ?",
        (tabela,),
    )
    return cursor.fetchone() is not None


def _mapa_colunas(cursor, tabela):
    return {c.strip().upper(): c for c in _listar_colunas(cursor, tabela)}


def _resolver_coluna(cursor, tabela, *candidatos):
    mapa = _mapa_colunas(cursor, tabela)
    for nome in candidatos:
        chave = str(nome).strip().upper()
        if chave in mapa:
            return mapa[chave]
    return None


def _coluna_existe(cursor, tabela, coluna):
    mapa = _mapa_colunas(cursor, tabela)
    return str(coluna).strip().upper() in mapa


def _montar_refs_colunas(cursor):
    """Resolve nomes físicos das colunas em Pessoa_MG (case/alias)."""
    def r(*keys):
        return _resolver_coluna(cursor, TABELA_DESTINO, *keys)

    return {
        'cpf': r('CPF_CNPJ'),
        'flag': r('FLAG'),
        'tipo': r('TIPO', 'Tipo'),
        'ocorrencia': r('OCORRENCIA'),
        'doc': r('PESSOA_DOCIDENTIFICADOR'),
        'id': r('IDTABELA'),
    }


def _alias_col(alias, nome_coluna):
    return f"{alias}.{_quote_col(nome_coluna)}"


def _listar_colunas(cursor, tabela):
    cursor.execute(
        """
        SELECT c.name
        FROM sys.columns c
        INNER JOIN sys.objects o ON c.object_id = o.object_id
        WHERE o.name = ? AND o.type = 'U'
        ORDER BY c.column_id
        """,
        (tabela,),
    )
    return [row[0] for row in cursor.fetchall()]


def _executar(cursor, sql, params=None):
    if params:
        cursor.execute(sql, params)
    else:
        cursor.execute(sql)


def garantir_tabela_staging(cursor, banco_gx, colunas_df, tabela_staging=None):
    """Garante tabela de staging com colunas do layout + IDtabela + Flag."""
    tabela_staging = tabela_staging or TABELA_STAGING
    db = _quote_db(banco_gx)
    tbl = _quote_table(tabela_staging)

    if not _tabela_existe(cursor, tabela_staging):
        cols_ddl = []
        for col in colunas_df:
            cols_ddl.append(f"{_quote_col(col)} VARCHAR(MAX) NULL")
        cols_ddl.append("[IDtabela] INT IDENTITY(1,1) NOT NULL")
        cols_ddl.append("[Flag] SMALLINT NULL")
        ddl = f"CREATE TABLE {db}.dbo.{tbl} ({', '.join(cols_ddl)})"
        _executar(cursor, ddl)
        logger.info("Tabela %s criada em %s", tabela_staging, banco_gx)
        return

    existentes = {c.strip() for c in _listar_colunas(cursor, tabela_staging)}
    for col in colunas_df:
        col_strip = col.strip()
        if col_strip not in existentes:
            _executar(
                cursor,
                f"ALTER TABLE {db}.dbo.{tbl} ADD {_quote_col(col_strip)} VARCHAR(MAX) NULL",
            )
    if 'IDtabela' not in existentes:
        _executar(
            cursor,
            f"ALTER TABLE {db}.dbo.{tbl} ADD [IDtabela] INT IDENTITY(1,1) NOT NULL",
        )
    if 'Flag' not in existentes:
        _executar(cursor, f"ALTER TABLE {db}.dbo.{tbl} ADD [Flag] SMALLINT NULL")


def inserir_staging(cursor, banco_gx, df, tabela_staging=None):
    """Trunca staging e insere dados validados em lote."""
    tabela_staging = tabela_staging or TABELA_STAGING
    db = _quote_db(banco_gx)
    tbl = _quote_table(tabela_staging)
    colunas = [c.strip() for c in df.columns if c.strip() not in ('IDtabela', 'Flag')]
    if not colunas:
        raise ValueError("Nenhuma coluna para importar.")

    _executar(cursor, f"TRUNCATE TABLE {db}.dbo.{tbl}")

    cols_sql = ', '.join(_quote_col(c) for c in colunas)
    placeholders = ', '.join(['?'] * len(colunas))
    sql = f"INSERT INTO {db}.dbo.{tbl} ({cols_sql}) VALUES ({placeholders})"

    registros = []
    for _, row in df.iterrows():
        valores = []
        for col in colunas:
            val = row[col]
            if pd.isna(val) or val is None:
                valores.append(None)
            else:
                valores.append(str(val).strip() if str(val).strip() != '' else None)
        registros.append(tuple(valores))

    if hasattr(cursor, 'fast_executemany'):
        cursor.fast_executemany = True

    chunk = 1000
    for i in range(0, len(registros), chunk):
        cursor.executemany(sql, registros[i:i + chunk])

    logger.info("%s registros inseridos em %s", len(registros), tabela_staging)
    return len(registros)


def criar_ou_recarregar_destino(cursor, banco_gx, tabela_destino, tabela_staging):
    """Recria tabela destino a partir do staging (DROP + SELECT INTO), sem copiar Flag/Ocorrencia."""
    db = _quote_db(banco_gx)
    dest = _quote_table(tabela_destino)
    staging = _quote_table(tabela_staging)

    if _tabela_existe(cursor, tabela_destino):
        _executar(cursor, f"DROP TABLE {db}.dbo.{dest}")
        logger.info("Tabela %s removida para recriação", tabela_destino)

    staging_cols = _listar_colunas(cursor, tabela_staging)
    ignorar = {'FLAG', 'OCORRENCIA'}
    data_cols = [c for c in staging_cols if c.strip().upper() not in ignorar]

    if not data_cols:
        raise ValueError(f"Staging sem colunas de dados para criar {tabela_destino}.")

    cols_sql = ', '.join(_quote_col(c) for c in data_cols)
    _executar(
        cursor,
        f"SELECT {cols_sql} INTO {db}.dbo.{dest} FROM {db}.dbo.{staging}",
    )
    logger.info(
        "Tabela %s criada com %s colunas (Flag/Ocorrencia serão preenchidos no pipeline)",
        tabela_destino, len(data_cols),
    )


def criar_ou_recarregar_pessoa_mg(cursor, banco_gx):
    """Sempre recria Pessoa_MG a partir do staging (DROP + SELECT INTO), sem copiar Flag."""
    criar_ou_recarregar_destino(cursor, banco_gx, TABELA_DESTINO, TABELA_STAGING)


def garantir_colunas_extra(cursor, banco_gx, tabela_destino=None, colunas_extra=None):
    tabela_destino = tabela_destino or TABELA_DESTINO
    colunas_extra = colunas_extra or COLUNAS_EXTRA_PESSOA_MG
    db = _quote_db(banco_gx)
    dest = _quote_table(tabela_destino)
    for coluna, tipo in colunas_extra:
        if not _coluna_existe(cursor, tabela_destino, coluna):
            _executar(
                cursor,
                f"ALTER TABLE {db}.dbo.{dest} ADD {_quote_col(coluna)} {tipo}",
            )


def _normalizar_campos_flag_1(cursor, gx, dest, refs):
    """Normaliza e-mail, crédito, MyHonda e segmentos (Flag = 1)."""
    col_flag = refs.get('flag')
    if not col_flag:
        return
    where_flag = f"WHERE {_alias_col('a', col_flag)} = 1"
    if _coluna_existe(cursor, TABELA_DESTINO, 'E_MAIL'):
        _executar(cursor, f"""
            UPDATE a
            SET a.E_MAIL = ISNULL(LEFT(LTRIM(RTRIM(a.E_MAIL)), 150), '')
            FROM {gx}.dbo.{dest} a
            {where_flag}
        """)

    if _coluna_existe(cursor, TABELA_DESTINO, 'EMAIL_ALTERNATIVO'):
        _executar(cursor, f"""
            UPDATE a
            SET a.EMAIL_ALTERNATIVO = ISNULL(LEFT(LTRIM(RTRIM(a.EMAIL_ALTERNATIVO)), 150), '')
            FROM {gx}.dbo.{dest} a
            {where_flag}
        """)

    if _coluna_existe(cursor, TABELA_DESTINO, 'LIM_CREDITO'):
        _executar(cursor, f"""
            UPDATE a
            SET a.LIM_CREDITO = CASE
                    WHEN ISNUMERIC(a.LIM_CREDITO) = 0 THEN 0
                    ELSE TRY_CONVERT(FLOAT, REPLACE(a.LIM_CREDITO, ',', '.'))
                END
            FROM {gx}.dbo.{dest} a
            {where_flag}
        """)

    if _coluna_existe(cursor, TABELA_DESTINO, 'LIM_CREDITO_VALIDADE'):
        _executar(cursor, f"""
            UPDATE a
            SET a.LIM_CREDITO_VALIDADE = CASE
                    WHEN ISDATE(REPLACE(a.LIM_CREDITO_VALIDADE, '/', '-')) = 0 THEN '1900-01-01'
                    ELSE REPLACE(a.LIM_CREDITO_VALIDADE, '/', '-')
                END
            FROM {gx}.dbo.{dest} a
            {where_flag}
              AND ISNUMERIC(a.LIM_CREDITO) = 1
        """)

    if (
        _coluna_existe(cursor, TABELA_DESTINO, 'Pessoa_CodigoMyHonda')
        and _coluna_existe(cursor, TABELA_DESTINO, 'CODIGO_MYHONDA')
    ):
        _executar(cursor, f"""
            UPDATE a
            SET a.Pessoa_CodigoMyHonda = CASE
                    WHEN ISNUMERIC(RTRIM(LTRIM(CODIGO_MYHONDA))) = 1
                        THEN CAST(RTRIM(LTRIM(CODIGO_MYHONDA)) AS BIGINT)
                    ELSE 0
                END
            FROM {gx}.dbo.{dest} a
            {where_flag}
        """)

    sets_segmento = []
    if _coluna_existe(cursor, TABELA_DESTINO, 'SEGMENTO_OFICINA'):
        sets_segmento.append("""
            a.SEGMENTO_OFICINA = CASE
                WHEN (a.SEGMENTO_OFICINA IS NULL OR a.SEGMENTO_OFICINA = '')
                THEN 'CLIENTE OFICINA'
                ELSE a.SEGMENTO_OFICINA
            END""")
    if _coluna_existe(cursor, TABELA_DESTINO, 'SEGMENTO_BALCAO'):
        sets_segmento.append("""
            a.SEGMENTO_BALCAO = CASE
                WHEN (a.SEGMENTO_BALCAO IS NULL OR a.SEGMENTO_BALCAO = '')
                THEN 'CLIENTE BALCÃO'
                ELSE a.SEGMENTO_BALCAO
            END""")
    if _coluna_existe(cursor, TABELA_DESTINO, 'SEGMENTO_VENDAS'):
        sets_segmento.append("""
            a.SEGMENTO_VENDAS = CASE
                WHEN (a.SEGMENTO_VENDAS IS NULL OR a.SEGMENTO_VENDAS = '')
                THEN 'CLIENTE VENDAS'
                ELSE a.SEGMENTO_VENDAS
            END""")

    if sets_segmento:
        _executar(cursor, f"""
            UPDATE a
            SET {','.join(sets_segmento)}
            FROM {gx}.dbo.{dest} a
            {where_flag}
        """)


def _inicializar_ocorrencia_e_flag(cursor, gx, dest, refs):
    """
    Primeiro passo do pipeline: zera Ocorrencia e define Flag (1/0) conforme CPF_CNPJ.
    """
    col_cpf = refs.get('cpf')
    col_flag = refs.get('flag')
    col_ocorrencia = refs.get('ocorrencia')

    if not col_cpf:
        raise ValueError("Coluna CPF_CNPJ não encontrada em Pessoa_MG.")

    if not col_flag:
        _executar(cursor, f"ALTER TABLE {gx}.dbo.{dest} ADD [Flag] SMALLINT NULL")
        col_flag = 'Flag'
        refs['flag'] = col_flag

    if not col_ocorrencia:
        _executar(cursor, f"ALTER TABLE {gx}.dbo.{dest} ADD [Ocorrencia] VARCHAR(500) NULL")
        col_ocorrencia = 'Ocorrencia'
        refs['ocorrencia'] = col_ocorrencia

    ac = _alias_col('a', col_ocorrencia)
    af = _alias_col('a', col_flag)
    cpf = _alias_col('a', col_cpf)

    _executar(cursor, f"UPDATE a SET {ac} = '' FROM {gx}.dbo.{dest} a")

    sql_flag = f"""
        UPDATE a
        SET {ac} = CASE
                WHEN {cpf} IS NULL OR RTRIM(LTRIM(CAST({cpf} AS VARCHAR(50)))) = ''
                THEN ISNULL({ac}, '') + ' | CPF CNPJ está vazio'
                ELSE {ac}
            END,
            {af} = CASE
                WHEN {cpf} IS NULL OR RTRIM(LTRIM(CAST({cpf} AS VARCHAR(50)))) = '' THEN 0
                ELSE 1
            END
        FROM {gx}.dbo.{dest} a
    """
    _executar(cursor, sql_flag)
    logger.info("Flag/Ocorrencia inicializados em Pessoa_MG (coluna Flag=%s)", col_flag)


def executar_pipeline_pos_carga(cursor, banco_gx, banco_wf):
    """Executa transformações pós-carga (ocorrências, doc, duplicidades, etc.)."""
    gx = _quote_db(banco_gx)
    wf = _quote_db(banco_wf)
    dest = _quote_table(TABELA_DESTINO)
    refs = _montar_refs_colunas(cursor)

    _inicializar_ocorrencia_e_flag(cursor, gx, dest, refs)

    col_cpf = refs['cpf']
    col_flag = refs['flag']
    col_tipo = refs.get('tipo')
    col_doc = refs.get('doc')
    col_ocorrencia = refs.get('ocorrencia')
    col_id = refs.get('id')

    cpf = _alias_col('a', col_cpf)
    flag = _alias_col('a', col_flag)
    where_flag = f"WHERE {flag} = 1"

    if col_doc:
        doc = _alias_col('a', col_doc)
        _executar(cursor, f"""
            UPDATE a
            SET {doc} = CASE
                    WHEN LEN(RTRIM(LTRIM({cpf}))) < 11
                        THEN REPLICATE('0', 11 - LEN(RTRIM(LTRIM({cpf})))) + RTRIM(LTRIM({cpf}))
                    WHEN LEN(RTRIM(LTRIM({cpf}))) > 11 AND LEN(RTRIM(LTRIM({cpf}))) < 14
                        THEN REPLICATE('0', 14 - LEN(RTRIM(LTRIM({cpf})))) + RTRIM(LTRIM({cpf}))
                    ELSE RTRIM(LTRIM({cpf}))
                END
            FROM {gx}.dbo.{dest} a
            {where_flag}
        """)

    if col_tipo and col_doc:
        tipo = _alias_col('a', col_tipo)
        _executar(cursor, f"""
            UPDATE a
            SET {tipo} = CASE
                    WHEN LEN(RTRIM(LTRIM({doc}))) <= 11 THEN 'F'
                    WHEN LEN(RTRIM(LTRIM({doc}))) > 11 AND LEN(RTRIM(LTRIM({cpf}))) <= 14 THEN 'J'
                    ELSE RTRIM(LTRIM({doc}))
                END
            FROM {gx}.dbo.{dest} a
            {where_flag}
              AND {tipo} NOT IN ('F', 'J', 'C', 'M', 'T', 'S', 'G', 'E')
        """)

    _normalizar_campos_flag_1(cursor, gx, dest, refs)

    oc = _alias_col('a', col_ocorrencia) if col_ocorrencia else None

    if _tabela_existe(cursor, 'Pessoa_EmProducao'):
        _executar(cursor, f"DROP TABLE {gx}.dbo.[Pessoa_EmProducao]")

    if col_doc:
        doc = _alias_col('a', col_doc)
        _executar(cursor, f"""
            SELECT a.*
            INTO {gx}.dbo.[Pessoa_EmProducao]
            FROM {gx}.dbo.{dest} a
            WHERE EXISTS (
                SELECT 1 FROM {wf}.dbo.[Pessoa] b
                WHERE {doc} = b.Pessoa_DocIdentificador COLLATE DATABASE_DEFAULT
            )
        """)

    if col_doc and col_flag and oc:
        _executar(cursor, f"""
            UPDATE a
            SET {flag} = 0,
                {oc} = ISNULL({oc}, '') + ' Cadastrado em WF-Produção.'
            FROM {gx}.dbo.{dest} a
            {where_flag}
              AND EXISTS (
                  SELECT 1 FROM {wf}.dbo.[Pessoa] b
                  WHERE {doc} = b.Pessoa_DocIdentificador COLLATE DATABASE_DEFAULT
              )
        """)

    if _tabela_existe(cursor, 'Cliente_Duplicados'):
        _executar(cursor, f"DROP TABLE {gx}.dbo.[Cliente_Duplicados]")

    if col_doc and col_id:
        doc_col = _quote_col(col_doc)
        id_col = _quote_col(col_id)
        _executar(cursor, f"""
            SELECT {doc_col}, COUNT(*) AS QUANTIDADE, MAX({id_col}) AS ID
            INTO {gx}.dbo.[Cliente_Duplicados]
            FROM {gx}.dbo.{dest}
            GROUP BY {doc_col}
            HAVING COUNT(*) > 1
        """)

        doc_q = _quote_col(col_doc)
        _executar(cursor, f"""
            UPDATE a
            SET {flag} = 0,
                {oc} = ISNULL({oc}, '') + ' | Duplicidades de CPF/CNPJ.'
            FROM {gx}.dbo.{dest} a
            INNER JOIN {gx}.dbo.[Cliente_Duplicados] b
                ON {doc} = b.{doc_q} COLLATE DATABASE_DEFAULT
            WHERE {_alias_col('a', col_id)} < b.ID
        """)

    if _tabela_existe(cursor, 'CPFCNPJ_NULL'):
        _executar(cursor, f"DROP TABLE {gx}.dbo.[CPFCNPJ_NULL]")

    if col_doc:
        _executar(cursor, f"""
            SELECT {_quote_col(col_id) if col_id else '[IDtabela]'}, [CODIGO_PESSOA], [NOME],
                   {_quote_col(col_tipo) if col_tipo else '[TIPO]'}, {_quote_col(col_cpf)}, {_quote_col(col_doc)}
            INTO {gx}.dbo.[CPFCNPJ_NULL]
            FROM {gx}.dbo.{dest}
            WHERE {_quote_col(col_doc)} IS NULL OR {_quote_col(col_doc)} = ''
        """)

        _executar(cursor, f"""
            UPDATE a
            SET {flag} = 0,
                {oc} = ISNULL({oc}, '') + ' | Registros de CPF/CNPJ NULOS.'
            FROM {gx}.dbo.{dest} a
            {where_flag}
              AND ({doc} IS NULL OR {doc} = '')
        """)

    if _coluna_existe(cursor, TABELA_DESTINO, 'BLOQUEIA_VENDA'):
        _executar(cursor, f"""
            UPDATE a
            SET a.Pessoa_BloqueiaVendaTituloAtraso = CASE
                    WHEN RTRIM(LTRIM(a.BLOQUEIA_VENDA)) = 'N' THEN 0
                    WHEN RTRIM(LTRIM(a.BLOQUEIA_VENDA)) = 'S' THEN 1
                    ELSE ISNULL(a.BLOQUEIA_VENDA, 0)
                END,
                a.Pessoa_BloqueiaEntradaOficina = CASE
                    WHEN RTRIM(LTRIM(a.BLOQUEIA_OFICINA)) = 'N' THEN 0
                    WHEN RTRIM(LTRIM(a.BLOQUEIA_OFICINA)) = 'S' THEN 1
                    ELSE ISNULL(a.BLOQUEIA_OFICINA, 0)
                END
            FROM {gx}.dbo.{dest} a
            {where_flag}
        """)

    if _coluna_existe(cursor, TABELA_DESTINO, 'DATA_CADASTRO'):
        _executar(cursor, f"""
            UPDATE a
            SET a.DATA_CADASTRO = CASE
                    WHEN ISDATE(REPLACE(a.DATA_CADASTRO, '/', '-')) = 0 THEN '1900-01-01'
                    WHEN (DATA_CADASTRO IS NULL OR DATA_CADASTRO = '') THEN CAST(GETDATE() AS date)
                    ELSE REPLACE(a.DATA_CADASTRO, '/', '-')
                END
            FROM {gx}.dbo.{dest} a
            {where_flag}
        """)


def obter_resumo_importacao(cursor, banco_gx, tabela_destino=None):
    tabela_destino = tabela_destino or TABELA_DESTINO
    gx = _quote_db(banco_gx)
    dest = _quote_table(tabela_destino)
    resumo = {'total': 0, 'flag_1': 0, 'flag_0': 0}

    if not _tabela_existe(cursor, tabela_destino):
        return resumo

    col_flag = _resolver_coluna(cursor, tabela_destino, 'Flag', 'FLAG')
    if not col_flag:
        _executar(cursor, f"SELECT COUNT(*) FROM {gx}.dbo.{dest}")
        resumo['total'] = cursor.fetchone()[0] or 0
        return resumo

    fq = _quote_col(col_flag)
    _executar(cursor, f"""
        SELECT COUNT(*),
               SUM(CASE WHEN {fq} = 1 THEN 1 ELSE 0 END),
               SUM(CASE WHEN {fq} = 0 THEN 1 ELSE 0 END)
        FROM {gx}.dbo.{dest}
    """)
    row = cursor.fetchone()
    if row:
        resumo['total'] = row[0] or 0
        resumo['flag_1'] = row[1] or 0
        resumo['flag_0'] = row[2] or 0
    return resumo


_COLUNAS_CHAVE_FLAG0 = (
    'CPF_CNPJ', 'NOME', 'CODIGO_PESSOA', 'PRODUTO_REFERENCIA', 'PRODUTO_DESCRICAO',
    'CHASSI', 'NUMERO_OS', 'CNPJ_EMPRESA', 'EMPRESA_CODIGO', 'TMO_REFERENCIA',
    'TITULO_NUMERO', 'TITULO_TIPO', 'IDTABELA', 'IDtabela',
)


def _colunas_chave_existentes(cursor, tabela_destino):
    encontradas = []
    vistos = set()
    for cand in _COLUNAS_CHAVE_FLAG0:
        col = _resolver_coluna(cursor, tabela_destino, cand)
        if col and col.upper() not in vistos:
            encontradas.append(col)
            vistos.add(col.upper())
        if len(encontradas) >= 5:
            break
    return encontradas


def obter_motivos_flag0(cursor, banco_gx, tabela_destino):
    """Agrupa registros Flag=0 pela mensagem de Ocorrencia."""
    gx = _quote_db(banco_gx)
    dest = _quote_table(tabela_destino)
    if not _tabela_existe(cursor, tabela_destino):
        return []

    col_flag = _resolver_coluna(cursor, tabela_destino, 'Flag', 'FLAG')
    col_ocorrencia = _resolver_coluna(cursor, tabela_destino, 'Ocorrencia', 'OCORRENCIA')
    if not col_flag:
        return []

    fq = _quote_col(col_flag)
    if col_ocorrencia:
        oq = _quote_col(col_ocorrencia)
        _executar(cursor, f"""
            SELECT
                CASE
                    WHEN {oq} IS NULL OR LTRIM(RTRIM({oq})) = ''
                    THEN N'(sem ocorrência registrada)'
                    ELSE LTRIM(RTRIM({oq}))
                END AS Motivo,
                COUNT(*) AS Qtd
            FROM {gx}.dbo.{dest}
            WHERE {fq} = 0
            GROUP BY
                CASE
                    WHEN {oq} IS NULL OR LTRIM(RTRIM({oq})) = ''
                    THEN N'(sem ocorrência registrada)'
                    ELSE LTRIM(RTRIM({oq}))
                END
            ORDER BY COUNT(*) DESC
        """)
    else:
        _executar(cursor, f"""
            SELECT N'(coluna Ocorrencia não existe na tabela)' AS Motivo, COUNT(*) AS Qtd
            FROM {gx}.dbo.{dest}
            WHERE {fq} = 0
        """)

    return [
        {'motivo': (row[0] or '').strip(), 'quantidade': int(row[1] or 0)}
        for row in cursor.fetchall()
    ]


def obter_detalhe_flag0(cursor, banco_gx, tabela_destino, limite=None):
    """Lista registros Flag=0 com colunas-chave + Ocorrencia."""
    gx = _quote_db(banco_gx)
    dest = _quote_table(tabela_destino)
    if not _tabela_existe(cursor, tabela_destino):
        return [], []

    col_flag = _resolver_coluna(cursor, tabela_destino, 'Flag', 'FLAG')
    if not col_flag:
        return [], []

    col_ocorrencia = _resolver_coluna(cursor, tabela_destino, 'Ocorrencia', 'OCORRENCIA')
    chaves = _colunas_chave_existentes(cursor, tabela_destino)

    cols_select = [_quote_col(c) for c in chaves]
    nomes = list(chaves)
    if col_ocorrencia:
        cols_select.append(_quote_col(col_ocorrencia))
        nomes.append(col_ocorrencia)
    else:
        cols_select.append("N'' AS [Ocorrencia]")
        nomes.append('Ocorrencia')

    top = f"TOP ({int(limite)}) " if limite else ""
    fq = _quote_col(col_flag)
    _executar(cursor, f"""
        SELECT {top}{', '.join(cols_select)}
        FROM {gx}.dbo.{dest}
        WHERE {fq} = 0
        ORDER BY 1
    """)

    rows = []
    for row in cursor.fetchall():
        item = {}
        for i, nome in enumerate(nomes):
            val = row[i]
            item[nome] = '' if val is None else str(val).strip()
        rows.append(item)
    return nomes, rows


def importar_forn_cli_para_base(df, banco_gx, banco_wf):
    """
    Importa DataFrame validado para DadosGX via up_01_Extrai_Pessoa_gx.
    Retorna (sucesso, mensagem, resumo_dict).
    """
    from db.connection import conectar_segunda_base
    from utils.importacao_procedures import executar_procedure_extracao, obter_config_procedure

    tipo_layout = 'forn_cli'
    cfg = obter_config_procedure(tipo_layout)
    tabela_staging = cfg['staging']
    tabela_destino = cfg['destino']

    banco_gx = _validar_identificador_sql(banco_gx.strip())
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
        garantir_tabela_staging(cursor, banco_gx, colunas_df, tabela_staging)
        total_inserido = inserir_staging(cursor, banco_gx, df, tabela_staging)

        executar_procedure_extracao(cursor, tipo_layout, banco_gx, banco_wf)

        from utils.importacao_depara_procedures import executar_depara_pos_importacao
        resumo_depara = executar_depara_pos_importacao(cursor, tipo_layout, banco_gx, banco_wf)
        conn.commit()
        resumo = obter_resumo_importacao(cursor, banco_gx, tabela_destino)
        resumo['inseridos_staging'] = total_inserido
        resumo['depara'] = resumo_depara
        resumo['procedure'] = cfg['procedure']
        msg = (
            f"Importação concluída em {banco_gx}.dbo.{tabela_destino} "
            f"(procedure {cfg['procedure']}): "
            f"{resumo['total']} registro(s), {resumo['flag_1']} importado(s) OK, "
            f"{resumo['flag_0']} rejeitado(s)."
        )
        return True, msg, resumo
    except Exception as e:
        conn.rollback()
        logger.exception("Erro na importação Forn_cli")
        return False, f"Erro na importação: {e}", {}
    finally:
        cursor.close()
        conn.close()
