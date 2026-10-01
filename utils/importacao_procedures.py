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
    _quote_col,
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
    'adiantamento': {
        'procedure': 'up_01_Extrai_Adiantamento_gx',
        'staging': 'Arquivo_Adiantamento_Tratado',
        'destino': 'FichaRazao_MG',
        'requer_wf': True,
    },
    'intercambiavel': {
        # up_13 trata Arquivo_Intercambiavel (coluna conteudo). O Python já grava
        # Arquivo_Intercambiavel_Tratado; a publicação segue o script legado de
        # ProdutoIntercambiavel_MG (não executa o up_13 para não DROP do Tratado).
        'procedure': 'up_13_Trata_Arquivo_Intercambiavel_gx',
        'staging': 'Arquivo_Intercambiavel_Tratado',
        'destino': 'ProdutoIntercambiavel_MG',
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
    'adiantamento',
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
    'adiantamento': [
        ('SALDO_Original', 'float NULL'),
        ('Pessoa_DocIdentificador', 'varchar(20) NULL'),
        ('TipoFichaRazao_Codigo', 'int NULL'),
        ('FichaRazao_PessoaCod', 'int NULL'),
        ('FichaRazao_EmpresaCod', 'int NULL'),
        ('Ocorrencia', 'varchar(500) NULL'),
    ],
    'intercambiavel': [
        ('ProdutoMarca_MarcaCod', 'int NULL'),
        ('Flag', 'int NULL'),
        ('Produto_Codigo', 'int NULL'),
        ('ProdutoIntercambiavel_ProdutoCod', 'int NULL'),
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


def _nome_backup_destino(nome_tabela):
    return f"{nome_tabela}__ImpAnterior"


def preservar_destino_oficial(cursor, nome_tabela):
    """Renomeia a tabela oficial para backup antes de republicar a partir da staging."""
    backup = _nome_backup_destino(nome_tabela)
    if _tabela_existe(cursor, backup):
        _executar(cursor, f"DROP TABLE dbo.{_quote_table(backup)}")
        logger.info("Backup anterior %s removido", backup)
    if not _tabela_existe(cursor, nome_tabela):
        return False
    _executar(
        cursor,
        "EXEC sp_rename ?, ?",
        (f"dbo.{nome_tabela}", backup),
    )
    logger.info("Tabela oficial %s preservada como %s", nome_tabela, backup)
    return True


def restaurar_destino_oficial(cursor, nome_tabela):
    """Devolve a tabela oficial a partir do backup se a publicação falhar."""
    backup = _nome_backup_destino(nome_tabela)
    if _tabela_existe(cursor, nome_tabela):
        _executar(cursor, f"DROP TABLE dbo.{_quote_table(nome_tabela)}")
        logger.info("Destino parcial %s removido após falha", nome_tabela)
    if _tabela_existe(cursor, backup):
        _executar(
            cursor,
            "EXEC sp_rename ?, ?",
            (f"dbo.{backup}", nome_tabela),
        )
        logger.info("Tabela oficial %s restaurada (não foi alterada pela importação falha)", nome_tabela)
        return True
    return False


def descartar_backup_destino(cursor, nome_tabela):
    backup = _nome_backup_destino(nome_tabela)
    if _tabela_existe(cursor, backup):
        _executar(cursor, f"DROP TABLE dbo.{_quote_table(backup)}")
        logger.info("Publicação concluída — backup %s descartado", backup)


def dropar_tabela_se_existir(cursor, banco_gx, nome_tabela):
    """Protege a tabela oficial: em vez de DROP imediato, renomeia para backup.

    Alteração mínima de transporte da publicação (não altera procedure nem validação).
    Se a extração falhar, restaurar_destino_oficial devolve a tabela original.
    """
    return preservar_destino_oficial(cursor, nome_tabela)


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

    try:
        colunas_extra = COLUNAS_EXTRA_POS_PRECRIAR.get(tipo_layout)
        # Sempre pré-cria o destino a partir da staging para layouts mapeados —
        # a procedure legada costuma falhar silenciosamente no SELECT INTO (Flag).
        if tipo_layout in LAYOUTS_PRECRIAR_DESTINO:
            precriar_destino_antes_procedure(cursor, banco_gx, cfg['staging'], destino)
            if colunas_extra:
                garantir_colunas_extra(
                    cursor, banco_gx, tabela_destino=destino, colunas_extra=colunas_extra,
                )
            try:
                cursor.connection.commit()
            except Exception:
                pass
            # dd/mm/aaaa → aaaa-mm-dd antes do CAST/ISDATE da procedure instalada.
            from utils.datas_layout import aplicar_datas_layout
            aplicar_datas_layout(cursor, tipo_layout, fonte='destino')

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

        # A procedure legada usa ISDATE (DATEFORMAT da sessão). Dia > 12 vira 1900-01-01.
        # Relê o texto original da staging (dd/mm/aaaa) e grava aaaa-mm-dd.
        from utils.datas_layout import aplicar_datas_layout
        aplicar_datas_layout(cursor, tipo_layout, fonte='staging')

        descartar_backup_destino(cursor, destino)
        logger.info("Procedure dbo.%s concluída → %s", proc, destino)
        return destino
    except Exception:
        try:
            restaurar_destino_oficial(cursor, destino)
        except Exception:
            logger.exception("Falha ao restaurar a tabela oficial %s", destino)
        raise


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
    try:
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

        descartar_backup_destino(cursor, destino)
        logger.info("Pipeline Produto concluído → %s", destino)
        return destino
    except Exception:
        try:
            restaurar_destino_oficial(cursor, destino)
        except Exception:
            logger.exception("Falha ao restaurar a tabela oficial %s", destino)
        raise


def _primeira_coluna(cursor, tabela, *candidatas):
    for nome in candidatas:
        if nome and _coluna_existe(cursor, tabela, nome):
            return nome
    return None


def executar_pipeline_intercambiavel(cursor, banco_gx, banco_wf):
    """
    Publica ProdutoIntercambiavel_MG a partir de Arquivo_Intercambiavel_Tratado.

    Segue o script legado (SELECT DISTINCT + Flag=1 + ProdutoMarca_MarcaCod).
    Não executa up_13_Trata_Arquivo_Intercambiavel_gx: ela DROP o Tratado e
    relê Arquivo_Intercambiavel.conteudo, que o Python não grava.
    """
    tipo_layout = 'intercambiavel'
    cfg = obter_config_procedure(tipo_layout)
    if not cfg:
        raise ValueError("Layout intercambiavel sem configuração.")

    banco_gx = _validar_identificador_sql(banco_gx.strip())
    banco_wf = _validar_identificador_sql((banco_wf or '').strip())
    if not banco_wf:
        raise ValueError("Pipeline Intercambiável exige BancoHomo (BancoWF).")

    staging = cfg['staging']
    destino = cfg['destino']
    gx = _quote_db(banco_gx)
    wf = _quote_db(banco_wf)
    stg = _quote_table(staging)
    dest = _quote_table(destino)

    if not _tabela_existe(cursor, staging):
        raise RuntimeError(
            f"Tabela de staging {staging} não existe em {banco_gx}. "
            "A carga do arquivo deve ocorrer antes da publicação."
        )

    col_cod = _primeira_coluna(cursor, staging, 'CODIGO_PRODUTO')
    col_ref = _primeira_coluna(cursor, staging, 'PRODUTO_REFERENCIA')
    col_cod_int = _primeira_coluna(
        cursor, staging,
        'PRODUTOINTERCAMBEAVEL_CODIGO', 'CODIGO_PRODUTOINTERCAMBEAVEL',
        'PRODUTOINTERCAMBIAVEL_CODIGO', 'CODIGO_PRODUTOINTERCAMBIAVEL',
    )
    col_ref_int = _primeira_coluna(
        cursor, staging,
        'PRODUTOINTERCAMBEAVEL_REFERENCIA', 'PRODUTO_REFERENCIAINTERCAMBEAVEL',
        'PRODUTOINTERCAMBIAVEL_REFERENCIA', 'PRODUTO_REFERENCIAINTERCAMBIAVEL',
    )
    col_marca = _primeira_coluna(
        cursor, staging, 'MARCA_DESCRICAO', 'MARCA', 'MARCA_CODIGO',
    )
    col_marca_cod = _primeira_coluna(cursor, staging, 'MARCA_CODIGO')
    col_id = _primeira_coluna(cursor, staging, 'IDtabela', 'idTabela', 'ID')

    if not col_cod or not col_ref:
        raise RuntimeError(
            f"Staging {staging} sem CODIGO_PRODUTO/PRODUTO_REFERENCIA."
        )

    pecas = [
        f"{_quote_col(col_cod)} AS [CODIGO_PRODUTO]",
        f"{_quote_col(col_ref)} AS [PRODUTO_REFERENCIA]",
    ]
    if col_cod_int:
        pecas.append(
            f"{_quote_col(col_cod_int)} AS [CODIGO_PRODUTOINTERCAMBEAVEL]"
        )
    if col_ref_int:
        pecas.append(
            f"{_quote_col(col_ref_int)} AS [PRODUTO_REFERENCIAINTERCAMBEAVEL]"
        )
    if col_marca:
        pecas.append(f"{_quote_col(col_marca)} AS [MARCA]")
    if col_marca_cod and col_marca_cod != col_marca:
        pecas.append(f"{_quote_col(col_marca_cod)} AS [MARCA_CODIGO]")
    if col_id:
        pecas.append(f"{_quote_col(col_id)} AS [idTabela]")

    dropar_tabela_se_existir(cursor, banco_gx, destino)
    try:
        _executar(
            cursor,
            f"SELECT DISTINCT {', '.join(pecas)} "
            f"INTO {gx}.dbo.{dest} FROM {gx}.dbo.{stg}",
        )
        if not _tabela_existe(cursor, destino):
            raise RuntimeError(
                f"Não foi possível criar {destino} a partir de {staging} em {banco_gx}."
            )

        colunas_extra = COLUNAS_EXTRA_POS_PRECRIAR.get(tipo_layout)
        if colunas_extra:
            garantir_colunas_extra(
                cursor, banco_gx, tabela_destino=destino, colunas_extra=colunas_extra,
            )

        if _coluna_existe(cursor, destino, 'Flag'):
            _executar(
                cursor,
                f"UPDATE {gx}.dbo.{dest} SET [Flag] = 1 WHERE [Flag] IS NULL",
            )

        if (
            _coluna_existe(cursor, destino, 'ProdutoMarca_MarcaCod')
            and _coluna_existe(cursor, destino, 'MARCA')
        ):
            if (
                _tabela_existe(cursor, 'Empresa_DePara')
                and _coluna_existe(cursor, 'Empresa_DePara', 'marc_ds')
                and _coluna_existe(cursor, 'Empresa_DePara', 'Empresa_MarcaCod')
            ):
                _executar(cursor, f"""
                    UPDATE p
                    SET p.[ProdutoMarca_MarcaCod] = d.[Empresa_MarcaCod]
                    FROM {gx}.dbo.{dest} p
                    INNER JOIN {gx}.dbo.[Empresa_DePara] d
                        ON d.[marc_ds] = p.[MARCA] COLLATE database_default
                    WHERE p.[ProdutoMarca_MarcaCod] IS NULL
                """)
            _executar(cursor, f"""
                UPDATE p
                SET p.[ProdutoMarca_MarcaCod] = d.[Marca_Codigo]
                FROM {gx}.dbo.{dest} p
                INNER JOIN {wf}.dbo.[Marca] d
                    ON d.[Marca_Descricao] = p.[MARCA] COLLATE database_default
                WHERE p.[ProdutoMarca_MarcaCod] IS NULL
            """)

        descartar_backup_destino(cursor, destino)
        logger.info("Pipeline Intercambiável concluído → %s", destino)
        return destino
    except Exception:
        try:
            restaurar_destino_oficial(cursor, destino)
        except Exception:
            logger.exception("Falha ao restaurar a tabela oficial %s", destino)
        raise
