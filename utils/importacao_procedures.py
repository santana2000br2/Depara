"""
Execução das stored procedures de extração (legado manual → importação automática).

Procedures instaladas no banco DadosGX do projeto (script 01_Create_PRC_Extrai_Pessoa_gx).
"""
from logger import logger
from utils.importacao_forn_cli import (
    COLUNAS_EXTRA_PESSOA_MG,
    _coluna_existe,
    _executar,
    _executar_procedure,
    _quote_db,
    _quote_table,
    _tabela_existe,
    _validar_identificador_sql,
    garantir_colunas_extra,
    garantir_pessoa_em_producao,
    popular_pessoa_docidentificador,
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
    'produto': {
        'procedure': 'up_01_Extrai_Produto_gx',
        'procedures': [
            'up_01_Extrai_Produto_gx',
            'up_02_Atualiza_Referencia_Produto_gx',
            'up_03_Trata_Duplicidade_Produto_gx',
            'up_04_Atualiza_Ocorrencia_Produto_gx',
        ],
        'staging': 'Arquivo_Produto_Tratado',
        'destino': 'Produto_MG',
        'requer_wf': True,
    },
    'produto_estoque': {
        'procedure': 'up_01_Extrai_ProdutoEstoque_gx',
        'staging': 'Arquivo_ProdutoEstoque_Tratado',
        'destino': 'ProdutoEstoque_MG',
        'requer_wf': False,
    },
    'prod_locacao': {
        'procedure': 'up_01_Extrai_ProdLocacao_gx',
        'staging': 'Arquivo_ProdLocacao_Tratado',
        'destino': 'ProdLocacao_MG',
        'requer_wf': True,
    },
    'movimento_estoque': {
        'procedure': 'up_01_Extrai_MovimentoEstoque_gx',
        'staging': 'Arquivo_MovimentoEstoque_Tratado',
        'destino': 'MovimentoEstoque_MG',
        'requer_wf': True,
    },
    'veiculo': {
        'procedure': 'up_01_Extrai_Veiculo_gx',
        'staging': 'Arquivo_Veiculo_Tratado',
        'destino': 'Veiculo_MG',
        'requer_wf': True,
    },
    'fseg_cab': {
        'procedure': 'up_01_Extrai_Fseg_Cab_gx',
        'staging': 'Arquivo_FSeg_Cab_Tratado',
        'destino': 'Ficha_Cab_MG',
        'requer_wf': True,
    },
    'fseg_prd': {
        'procedure': 'up_01_Extrai_Fseg_Prd_gx',
        'staging': 'Arquivo_FSeg_Prd_Tratado',
        'destino': 'Ficha_Prd_MG',
        'requer_wf': True,
    },
    'fseg_srv': {
        'procedure': 'up_01_Extrai_Fseg_Srv_gx',
        'staging': 'Arquivo_FSeg_Srv_Tratado',
        'destino': 'Ficha_Srv_MG',
        'requer_wf': True,
    },
    'financeiro': {
        'procedure': 'up_01_Extrai_Financeiro_gx',
        'staging': 'Arquivo_Financeiro_Tratado',
        'destino': 'Titulo_MG',
        'requer_wf': True,
    },
}

# Layouts cujas procedures legadas fazem SELECT A.*, 1 AS Flag (colide com Flag da staging).
LAYOUTS_PRECRIAR_DESTINO = frozenset({
    'forn_cli_endereco',
    'forn_cli_documento',
    'forn_cli_enquadramento',
    'forn_cli_telefone',
    'forn_cli_contato',
    'produto',
    'produto_estoque',
    'prod_locacao',
    'movimento_estoque',
    'veiculo',
    'fseg_cab',
    'fseg_prd',
    'fseg_srv',
    'financeiro',
})

# Colunas extras do script original (ALTER após o SELECT INTO).
# No legado de Documento/Telefone/etc. o ALTER fica DENTRO do IF NOT EXISTS da tabela;
# se o Python pré-cria o destino, essas colunas nunca são criadas — por isso
# sempre aplicamos depois da procedure.
COLUNAS_EXTRA_POS_PRECRIAR = {
    'forn_cli': list(COLUNAS_EXTRA_PESSOA_MG),
    'forn_cli_endereco': [
        ('Pessoa_DocIdentificador', 'varchar(20) NULL'),
        ('Municipio_Codigo', 'smallint NULL'),
        ('Estado_Codigo', 'char(2) NULL'),
        ('Pais_Codigo', 'smallint NULL'),
        ('TipoLogradouro_Codigo', 'smallint NULL'),
        ('Ocorrencia', 'VARCHAR(500) NULL'),
    ],
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
    'forn_cli_telefone': [
        ('Pessoa_DocIdentificador', 'varchar(20) NULL'),
        ('Ocorrencia', 'VARCHAR(500) NULL'),
    ],
    'forn_cli_contato': [
        ('Pessoa_DocIdentificador', 'varchar(20) NULL'),
        ('Ocorrencia', 'VARCHAR(500) NULL'),
    ],
    'produto': [
        ('Empresa_Codigo', 'int NULL'),
        ('PRODUTO_REFERENCIA_Ajustado', 'nvarchar(510) NULL'),
        ('PRODUTO_REFERENCIATRANS', 'nvarchar(510) NULL'),
        ('Produto_NCMCod', 'int NULL'),
        ('Unidade', 'int NULL'),
        ('TipoProduto', 'int NULL'),
        ('GrupoProduto', 'int NULL'),
        ('GrupoLucratividadeCodigo', 'int NULL'),
        ('ProcedenciaCodigo', 'int NULL'),
        ('ProdutoMarca_MarcaCod', 'int NULL'),
        ('ProdutoPreco_TabelaPrecoCod_GA', 'int NULL'),
        ('ProdutoPreco_TabelaPrecoCod_PP', 'int NULL'),
        ('ProdutoPreco_TabelaPrecoCod_PS', 'int NULL'),
        ('ProdutoPreco_TabelaPrecoCod_RP', 'int NULL'),
        ('ProdutoPreco_TabelaPrecoCod_ES', 'int NULL'),
        ('Produto_CodigoWF', 'int NULL'),
        ('Ocorrencia', 'varchar(500) NULL'),
    ],
    'produto_estoque': [
        ('Emp_Ds', 'varchar(200) NULL'),
        ('ProdutoEstoque_EmpresaCod', 'int NULL'),
        ('ProdutoEstoque_EstoqueCod', 'int NULL'),
        ('Produto_Codigo', 'int NULL'),
        ('PRODUTO_REFERENCIA_Ajustado', 'varchar(30) NULL'),
        ('PRODUTO_REFERENCIATRANS', 'varchar(30) NULL'),
        ('ProdutoMarca_MarcaCod', 'varchar(30) NULL'),
        ('Ocorrencia', 'varchar(500) NULL'),
    ],
    'prod_locacao': [
        ('ProdutoEstoque_EmpresaCod', 'int NULL'),
        ('ProdutoEstoque_EstoqueCod', 'int NULL'),
        ('ProdutoMarca_MarcaCod', 'int NULL'),
        ('PRODUTO_REFERENCIA_Ajustado', 'nvarchar(510) NULL'),
        ('PRODUTO_REFERENCIATRANS', 'nvarchar(510) NULL'),
        ('Produto_CodigoWF', 'int NULL'),
        ('ProdutoEstoqueLocalizacao_LocalProdutoCod', 'int NULL'),
        ('ProdutoEstoqueLocalizacao_Tipo', 'char(1) NULL'),
        ('Ocorrencia', 'varchar(500) NULL'),
    ],
    'movimento_estoque': [
        ('MovimentoEstoque_NaturezaOperacaoCod', 'int NULL'),
        ('Estoque_CodigoWF', 'int NULL'),
        ('Departamento_CodigoWF', 'int NULL'),
        ('MovimentoEstoque_EmpresaCod', 'int NULL'),
        ('Produto_CodigoWF', 'int NULL'),
        ('Referencia', 'varchar(30) NULL'),
        ('ProdutoMarca_ReferenciaAlfanumerico', 'varchar(30) NULL'),
        ('ProdutoMarca_MarcaCod', 'int NULL'),
        ('TipoProdutoCod', 'int NULL'),
        ('mult', 'int NULL'),
        ('Pessoa_DocIdentificador', 'varchar(20) NULL'),
        ('MovimentoEstoque_PessoaCod', 'int NULL'),
        ('Ocorrencia', 'varchar(500) NULL'),
    ],
    'veiculo': [
        # Colunas WF/derivadas
        ('Ve_FabMod', 'varchar(10) NULL'),
        ('Marca_CodigoWF', 'int NULL'),
        ('ModeloVeiculoWF', 'int NULL'),
        ('CorInternaWF', 'int NULL'),
        ('CorExternaWF', 'int NULL'),
        ('VeiculoAno', 'int NULL'),
        ('Veiculo_Status', 'char(1) NULL'),
        ('Empresa_Codigo', 'int NULL'),
        ('VeiculoProprietario', 'int NULL'),
        ('Veiculo_PessoaCodConcessionaria', 'int NULL'),
        ('Pessoa_DocIdentificador', 'varchar(20) NULL'),
        ('Veiculo_EstadoCod_Placa', 'char(2) NULL'),
        ('Veiculo_MunicipioCod_Placa', 'int NULL'),
        ('WMI_VIN', 'varchar(3) NULL'),
        ('WMI_Fabricante', 'varchar(150) NULL'),
        ('Maquina_Implemento', 'int NULL'),
        ('Ocorrencia', 'varchar(500) NULL'),
        # Colunas de origem opcionais (podem não vir no arquivo) usadas pela
        # extração e pelas procedures De/Para — garantidas para evitar
        # "Invalid column name" quando ausentes no layout.
        ('CODIGO_LINHA', 'nvarchar(510) NULL'),
        ('COR_INTERNA_CODIGO', 'varchar(20) NULL'),
        ('COR_INTERNA_DESCRICAO', 'varchar(50) NULL'),
        ('ESTADO_PLACA', 'varchar(2) NULL'),
        ('MUNICIPIO_PLACA', 'varchar(100) NULL'),
        ('PLACA', 'varchar(10) NULL'),
        ('CPF_CNPJ', 'varchar(20) NULL'),
        ('KM', 'varchar(20) NULL'),
        ('RENAVAM', 'varchar(20) NULL'),
        ('SERIE', 'varchar(50) NULL'),
        ('DATA_VENDA', 'varchar(20) NULL'),
    ],
    'fseg_cab': [
        ('Veiculo_Codigo', 'int NULL'),
        ('Empresa_Codigo', 'int NULL'),
        ('Pessoa_DocIdentificador', 'varchar(20) NULL'),
        ('Pessoa_Codigo', 'int NULL'),
        ('TipoOSCod', 'int NULL'),
        ('Maquina_Implemento', 'int NULL'),
        ('Ocorrencia', 'varchar(500) NULL'),
        # Nome sem acento usado na procedure legada
        ('DATA_LIBERACAO', 'varchar(20) NULL'),
    ],
    'fseg_prd': [
        ('Veiculo_Codigo', 'int NULL'),
        ('Produto_Codigo', 'int NULL'),
        ('ProdutoMarca_MarcaCod', 'int NULL'),
        ('PRODUTO_REFERENCIA_Ajustado', 'nvarchar(510) NULL'),
        ('PRODUTO_REFERENCIATRANS', 'nvarchar(510) NULL'),
        ('Empresa_Codigo', 'int NULL'),
        ('TipoOSCod', 'int NULL'),
        ('Ocorrencia', 'varchar(500) NULL'),
    ],
    'fseg_srv': [
        ('Veiculo_Codigo', 'int NULL'),
        ('TMO_CodigoWF', 'int NULL'),
        ('TMO_DescricaoWF', 'varchar(50) NULL'),
        ('Empresa_Codigo', 'int NULL'),
        ('TipoOSCod', 'int NULL'),
        ('Ocorrencia', 'varchar(500) NULL'),
    ],
    'financeiro': [
        ('Empresa_Codigo', 'int NULL'),
        ('Pessoa_DocIdentificador', 'varchar(20) NULL'),
        ('Pessoa_Codigo', 'int NULL'),
        ('NaturezaOperacao_CodigoWF', 'int NULL'),
        ('TipoTitulo_CodigoWF', 'int NULL'),
        ('AgenteCobrador_CodigoWF', 'int NULL'),
        ('ContaGerencial_CodigoWF', 'int NULL'),
        ('Departamento_CodigoWF', 'int NULL'),
        ('TipoCobranca_CodigoWF', 'int NULL'),
        ('Banco_CodigoWF', 'int NULL'),
        ('Titulo_NSU', 'varchar(20) NULL'),
        ('TITULO_NUMERO_new', 'nvarchar(510) NULL'),
        ('Ocorrencia', 'varchar(500) NULL'),
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
    tbl = _quote_table(nome_tabela)
    # Conexão já está no DadosGX — usar dbo. evita falha com nome em 3 partes.
    _executar(cursor, f"DROP TABLE dbo.{tbl}")
    logger.info("Tabela %s removida antes da procedure", nome_tabela)
    return True


def precriar_destino_antes_procedure(cursor, banco_gx, tabela_staging, tabela_destino):
    """
    Cria destino a partir do staging antes da procedure legada.

    Procedures como up_02 usam ``SELECT A.*, 1 AS Flag INTO ...``; a staging já
    possui coluna Flag (criada pelo Python), o que gera erro 2705 silencioso
    dentro de sp_executesql. Pré-criar com SELECT * (como up_01) evita isso.
    """
    staging = _quote_table(tabela_staging)
    dest = _quote_table(tabela_destino)

    if not _tabela_existe(cursor, tabela_staging):
        raise RuntimeError(
            f"Staging dbo.{tabela_staging} não existe em {banco_gx} — "
            f"impossível pré-criar {tabela_destino}."
        )

    if _tabela_existe(cursor, tabela_destino):
        _executar(cursor, f"DROP TABLE dbo.{dest}")

    _executar(
        cursor,
        f"SELECT * INTO dbo.{dest} FROM dbo.{staging} WHERE 1=1",
    )

    if not _tabela_existe(cursor, tabela_destino):
        raise RuntimeError(
            f"Falha ao pré-criar dbo.{tabela_destino} a partir de dbo.{tabela_staging} "
            f"em {banco_gx}."
        )

    # Flag vem NULL da staging; procedure espera Flag numérico.
    if _coluna_existe(cursor, tabela_destino, 'Flag'):
        _executar(
            cursor,
            f"UPDATE dbo.{dest} SET Flag = 1 WHERE Flag IS NULL",
        )

    logger.info(
        "Tabela %s pré-criada a partir de %s (%s linha(s))",
        tabela_destino,
        tabela_staging,
        _contar_staging(cursor, tabela_destino),
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
        # Última tentativa: a carga deveria ter criado; evita 42S02 opaco na procedure.
        raise RuntimeError(
            f"Tabela de staging dbo.{cfg['staging']} não existe em {banco_gx}. "
            "A carga do arquivo deve criar essa tabela antes da procedure. "
            "Reinicie o aplicativo e importe novamente."
        )

    from utils.importacao_pessoa_mg_dependencia import (
        LAYOUTS_DEPENDEM_PESSOA_MG,
        LAYOUTS_PESSOA_MG_OPCIONAL,
        validar_staging_depende_pessoa_mg,
    )
    from utils.importacao_produto_mg_dependencia import (
        LAYOUTS_DEPENDEM_PRODUTO_MG,
        validar_staging_depende_produto_mg,
    )
    from utils.importacao_veiculo_mg_dependencia import (
        LAYOUTS_DEPENDEM_VEICULO_MG,
        validar_staging_depende_veiculo_mg,
    )
    from utils.importacao_ficha_cab_mg_dependencia import (
        LAYOUTS_DEPENDEM_FICHA_CAB_MG,
        validar_staging_depende_ficha_cab_mg,
    )
    if tipo_layout in LAYOUTS_DEPENDEM_PESSOA_MG and tipo_layout not in LAYOUTS_PESSOA_MG_OPCIONAL:
        validar_staging_depende_pessoa_mg(cursor, cfg['staging'])
    if tipo_layout in LAYOUTS_DEPENDEM_PESSOA_MG or str(tipo_layout).startswith('forn_cli_'):
        garantir_pessoa_em_producao(cursor)
    if tipo_layout in LAYOUTS_DEPENDEM_PRODUTO_MG:
        validar_staging_depende_produto_mg(cursor, cfg['staging'])
    if tipo_layout in LAYOUTS_DEPENDEM_VEICULO_MG:
        validar_staging_depende_veiculo_mg(cursor, cfg['staging'])
    if tipo_layout in LAYOUTS_DEPENDEM_FICHA_CAB_MG:
        validar_staging_depende_ficha_cab_mg(cursor, cfg['staging'])

    dropar_tabela_se_existir(cursor, banco_gx, destino)

    colunas_extra = COLUNAS_EXTRA_POS_PRECRIAR.get(tipo_layout)
    # Sempre pré-cria o destino a partir da staging para layouts mapeados —
    # a procedure legada costuma falhar silenciosamente no SELECT INTO (Flag).
    if tipo_layout in LAYOUTS_PRECRIAR_DESTINO:
        precriar_destino_antes_procedure(cursor, banco_gx, cfg['staging'], destino)
        if colunas_extra:
            garantir_colunas_extra(
                cursor, banco_gx, tabela_destino=destino, colunas_extra=colunas_extra,
            )
        # Confirma staging+destino antes do EXEC: se a procedure abortar a
        # transação, a carga do arquivo não é desfeita.
        try:
            cursor.connection.commit()
        except Exception:
            pass

    logger.info("Executando dbo.%s (@BancoDadosGX=%s)", proc, banco_gx)

    try:
        if cfg.get('requer_wf'):
            if not banco_wf:
                raise ValueError(f"Procedure {proc} exige @BancoWF.")
            banco_wf = _validar_identificador_sql(banco_wf.strip())
            _executar_procedure(cursor, f"EXEC dbo.{proc} ?, ?", (banco_gx, banco_wf))
        elif tipo_layout == 'forn_cli_telefone':
            _executar_procedure(cursor, f"EXEC dbo.{proc} ?, ?", (banco_gx, ddd_padrao))
        else:
            _executar_procedure(cursor, f"EXEC dbo.{proc} ?", (banco_gx,))
    except Exception as exc:
        logger.exception("Procedure dbo.%s falhou: %s", proc, exc)
        try:
            cursor.connection.rollback()
        except Exception:
            pass
        if not _tabela_existe(cursor, destino):
            raise
        logger.warning(
            "Procedure dbo.%s falhou, mas %s já existe — extração segue (colunas extras / DocIdentificador)",
            proc, destino,
        )

    if not _tabela_existe(cursor, destino):
        n_staging = _contar_staging(cursor, cfg['staging'])
        logger.warning(
            "Procedure %s concluiu sem %s — recriando a partir da staging (%s regs)",
            proc, destino, n_staging,
        )
        precriar_destino_antes_procedure(cursor, banco_gx, cfg['staging'], destino)
        if colunas_extra:
            garantir_colunas_extra(
                cursor, banco_gx, tabela_destino=destino, colunas_extra=colunas_extra,
            )

    if not _tabela_existe(cursor, destino):
        n_staging = _contar_staging(cursor, cfg['staging'])
        raise RuntimeError(
            f"Procedure {proc} concluiu, mas {destino} não foi criada em {banco_gx}. "
            f"Staging possui {n_staging} registro(s). "
            "Falha ao pré-criar o destino a partir da staging."
        )

    colunas_extra = COLUNAS_EXTRA_POS_PRECRIAR.get(tipo_layout)
    if colunas_extra:
        garantir_colunas_extra(
            cursor, banco_gx, tabela_destino=destino, colunas_extra=colunas_extra,
        )
    if tipo_layout == 'forn_cli' or str(tipo_layout).startswith('forn_cli_'):
        popular_pessoa_docidentificador(cursor, destino)

    logger.info("Procedure dbo.%s concluída → %s", proc, destino)
    return destino


def executar_pipeline_produto(cursor, banco_gx, banco_wf):
    """
    Executa o pipeline de extração Produto (up_01 a up_04) após staging carregada.
    """
    tipo_layout = 'produto'
    cfg = obter_config_procedure(tipo_layout)
    if not cfg:
        raise ValueError("Layout produto sem configuração de procedures.")

    banco_gx = _validar_identificador_sql(banco_gx.strip())
    banco_wf = _validar_identificador_sql((banco_wf or '').strip())
    if not banco_wf:
        raise ValueError("Pipeline Produto exige @BancoWF.")

    staging = cfg['staging']
    destino = cfg['destino']

    if not _tabela_existe(cursor, staging):
        raise RuntimeError(
            f"Tabela de staging {staging} não existe em {banco_gx}. "
            "A carga do arquivo deve ocorrer antes das procedures."
        )

    dropar_tabela_se_existir(cursor, banco_gx, destino)
    precriar_destino_antes_procedure(cursor, banco_gx, staging, destino)
    colunas_extra = COLUNAS_EXTRA_POS_PRECRIAR.get(tipo_layout)
    if colunas_extra:
        garantir_colunas_extra(
            cursor, banco_gx, tabela_destino=destino, colunas_extra=colunas_extra,
        )

    procedures = cfg.get('procedures') or [cfg['procedure']]
    for proc in procedures:
        if not procedure_existe(cursor, proc):
            raise RuntimeError(
                f"Procedure dbo.{proc} não encontrada em {banco_gx}. "
                "Instale os scripts em procedure/Produto/ no banco DadosGX "
                "e execute up_Replace_Name_DadosGx_Procedures com o nome do banco."
            )
        logger.info("Executando dbo.%s (@BancoDadosGX=%s, @BancoWF=%s)", proc, banco_gx, banco_wf)
        _executar_procedure(cursor, f"EXEC dbo.{proc} ?, ?", (banco_gx, banco_wf))

    if not _tabela_existe(cursor, destino):
        n_staging = _contar_staging(cursor, staging)
        raise RuntimeError(
            f"Pipeline Produto concluiu, mas {destino} não existe em {banco_gx}. "
            f"Staging possui {n_staging} registro(s)."
        )

    logger.info("Pipeline Produto concluído → %s", destino)
    return destino
