"""
Execução das stored procedures de extração (legado manual → importação automática).

Procedures instaladas no banco DadosGX do projeto (script 01_Create_PRC_Extrai_Pessoa_gx).
"""
from logger import logger
from utils.importacao_forn_cli import (
    _executar,
    _quote_db,
    _quote_table,
    _tabela_existe,
    _validar_identificador_sql,
    garantir_colunas_extra,
)

# Mapeamento layout → procedure / tabelas (conforme script legado)
CONFIG_PROCEDURES = {
    'forn_cli': {
        'procedure': 'up_01_Extrai_Pessoa_gx',
        'staging': 'Arquivo_Forn_cli_Tratado',
        'destino': 'Pessoa_MG',
        'requer_wf': True,
    },
    'forn_cli_documento': {
        'procedure': 'up_03_Extrai_PessoaDoc_gx',
        'staging': 'Arquivo_Forn_Cli_Documento_Tratado',
        'destino': 'PessoaDocumento_MG',
        'requer_wf': False,
    },
    'forn_cli_endereco': {
        'procedure': 'up_02_Extrai_PessoaEndereco_gx',
        'staging': 'Arquivo_Forn_Cli_Endereco_Tratado',
        'destino': 'PessoaEndereco_MG',
        'requer_wf': False,
    },
    'forn_cli_enquadramento': {
        'procedure': 'up_04_Extrai_PessoaEnquadramento_gx',
        'staging': 'Arquivo_Forn_Cli_Enquadramento_Tratado',
        'destino': 'PessoaEnquadramento_MG',
        'requer_wf': False,
    },
    'forn_cli_telefone': {
        'procedure': 'up_05_Extrai_PessoaTelefone_gx',
        'staging': 'Arquivo_Forn_Cli_Telefone_Tratado',
        'destino': 'PessoaTelefone_MG',
        'requer_wf': False,
        'params_extra': ('ddd_padrao',),
    },
    'forn_cli_contato': {
        'procedure': 'up_06_Extrai_PessoaContato_gx',
        'staging': 'Arquivo_Forn_cli_Contato_Tratado',
        'destino': 'PessoaContato_MG',
        'requer_wf': False,
    },
}

# Layouts cujas procedures legadas fazem SELECT A.*, 1 AS Flag (colide com Flag da staging).
LAYOUTS_PRECRIAR_DESTINO = frozenset({
    'forn_cli_endereco',
    'forn_cli_documento',
    'forn_cli_enquadramento',
})

# Colunas que as procedures legadas só criam dentro do IF NOT EXISTS (SELECT INTO).
COLUNAS_EXTRA_POS_PRECRIAR = {
    'forn_cli_documento': [
        ('Pessoa_DocIdentificador', 'varchar(20) NULL'),
        ('Ocorrencia', 'VARCHAR(500) NULL'),
    ],
    'forn_cli_enquadramento': [
        ('Pessoa_DocIdentificador', 'varchar(20) NULL'),
        ('Municipio_Codigo', 'int NULL'),
        ('Estado_Codigo', 'varchar(10) NULL'),
        ('Data_Cadastro', 'date NULL'),
        ('Ocorrencia', 'VARCHAR(500) NULL'),
    ],
}


def obter_config_procedure(tipo_layout):
    return CONFIG_PROCEDURES.get(tipo_layout)


def procedure_existe(cursor, nome_procedure):
    cursor.execute(
        "SELECT 1 FROM sys.procedures WHERE name = ? AND type = 'P'",
        (nome_procedure.strip(),),
    )
    return cursor.fetchone() is not None


def dropar_tabela_se_existir(cursor, banco_gx, nome_tabela):
    """Remove tabela destino para a procedure recriar (legado só faz SELECT INTO se não existir)."""
    if not _tabela_existe(cursor, nome_tabela):
        return False
    db = _quote_db(banco_gx)
    tbl = _quote_table(nome_tabela)
    _executar(cursor, f"DROP TABLE {db}.dbo.{tbl}")
    logger.info("Tabela %s removida antes da procedure", nome_tabela)
    return True


def precriar_destino_antes_procedure(cursor, banco_gx, tabela_staging, tabela_destino):
    """
    Cria destino a partir do staging antes da procedure legada.

    Procedures como up_02 usam ``SELECT A.*, 1 AS Flag INTO ...``; a staging já
    possui coluna Flag (criada pelo Python), o que gera erro 2705 silencioso
    dentro de sp_executesql. Pré-criar com SELECT * (como up_01) evita isso.
    """
    db = _quote_db(banco_gx)
    staging = _quote_table(tabela_staging)
    dest = _quote_table(tabela_destino)
    _executar(
        cursor,
        f"SELECT * INTO {db}.dbo.{dest} FROM {db}.dbo.{staging} WHERE 1=1",
    )
    logger.info(
        "Tabela %s pré-criada a partir de %s (procedure legada pula SELECT INTO)",
        tabela_destino,
        tabela_staging,
    )


def _contar_staging(cursor, tabela_staging):
    tbl = _quote_table(tabela_staging)
    cursor.execute(f"SELECT COUNT(*) FROM dbo.{tbl}")
    row = cursor.fetchone()
    return row[0] if row else 0


def executar_procedure_extracao(cursor, tipo_layout, banco_gx, banco_wf=None, ddd_padrao='11'):
    """
    Executa a procedure de extração do layout no banco DadosGX conectado.
    A conexão deve estar apontando para o banco DadosGX do projeto.
    """
    cfg = obter_config_procedure(tipo_layout)
    if not cfg:
        raise ValueError(f"Layout '{tipo_layout}' sem procedure configurada.")

    proc = cfg['procedure']
    destino = cfg['destino']
    banco_gx = _validar_identificador_sql(banco_gx.strip())

    if not procedure_existe(cursor, proc):
        raise RuntimeError(
            f"Procedure dbo.{proc} não encontrada em {banco_gx}. "
            "Instale o script 01_Create_PRC_Extrai_Pessoa_gx no banco DadosGX "
            "e execute up_Replace_Name_DadosGx_Procedures com o nome do banco."
        )

    if not _tabela_existe(cursor, cfg['staging']):
        raise RuntimeError(
            f"Tabela de staging {cfg['staging']} não existe em {banco_gx}. "
            "A carga do arquivo deve ocorrer antes da procedure."
        )

    from utils.importacao_pessoa_mg_dependencia import (
        LAYOUTS_DEPENDEM_PESSOA_MG,
        validar_staging_depende_pessoa_mg,
    )
    if tipo_layout in LAYOUTS_DEPENDEM_PESSOA_MG:
        validar_staging_depende_pessoa_mg(cursor, cfg['staging'])

    dropar_tabela_se_existir(cursor, banco_gx, destino)

    if tipo_layout in LAYOUTS_PRECRIAR_DESTINO:
        precriar_destino_antes_procedure(cursor, banco_gx, cfg['staging'], destino)
        colunas_extra = COLUNAS_EXTRA_POS_PRECRIAR.get(tipo_layout)
        if colunas_extra:
            garantir_colunas_extra(
                cursor, banco_gx, tabela_destino=destino, colunas_extra=colunas_extra,
            )

    logger.info("Executando dbo.%s (@BancoDadosGX=%s)", proc, banco_gx)

    if cfg.get('requer_wf'):
        if not banco_wf:
            raise ValueError(f"Procedure {proc} exige @BancoWF.")
        banco_wf = _validar_identificador_sql(banco_wf.strip())
        _executar(cursor, f"EXEC dbo.{proc} ?, ?", (banco_gx, banco_wf))
    elif tipo_layout == 'forn_cli_telefone':
        _executar(cursor, f"EXEC dbo.{proc} ?, ?", (banco_gx, ddd_padrao))
    else:
        _executar(cursor, f"EXEC dbo.{proc} ?", (banco_gx,))

    if not _tabela_existe(cursor, destino):
        n_staging = _contar_staging(cursor, cfg['staging'])
        raise RuntimeError(
            f"Procedure {proc} concluiu, mas {destino} não foi criada em {banco_gx}. "
            f"Staging possui {n_staging} registro(s). "
            "Causa provável: falha silenciosa no SELECT INTO da procedure legada "
            "(coluna Flag duplicada). Atualize o aplicativo ou reinstale a procedure corrigida."
        )

    logger.info("Procedure dbo.%s concluída → %s", proc, destino)
    return destino
