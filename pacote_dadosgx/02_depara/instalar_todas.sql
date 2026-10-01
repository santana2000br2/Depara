-- =============================================================================
-- INSTALAR — Procedures De/Para (DadosGX)
-- Instalador em T-SQL puro (NAO precisa SQLCMD Mode).
-- Antes de executar: substitua [DadosGX_SeuProjeto] pelo nome real do banco.
-- Gerado em: 01/10/2026 08:20
-- =============================================================================

USE [DadosGX_SeuProjeto];  -- ALTERE para o nome real do banco DadosGX
GO


-- >>> INICIO: 00_drop_procedures.sql
-- =============================================================================
-- DROP — Procedures De/Para (DadosGX)
-- Pacote DadosGX — gerar drop antes da reinstalacao
-- Gerado em: 01/10/2026 08:20
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Pessoa_DePara_SegmentoMercado' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_01_Pessoa_DePara_SegmentoMercado];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_02_Pessoa_DePara_Escolaridade' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_02_Pessoa_DePara_Escolaridade];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_03_Pessoa_DePara_Profissao' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_03_Pessoa_DePara_Profissao];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_04_Pessoa_DePara_EstadoCivil' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_04_Pessoa_DePara_EstadoCivil];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_05_Pessoa_DePara_Municipio' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_05_Pessoa_DePara_Municipio];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_06_Pessoa_DePara_TipoLogradouro' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_06_Pessoa_DePara_TipoLogradouro];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_07_Pessoa_DePara_Estado_Pais' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_07_Pessoa_DePara_Estado_Pais];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_08_Pessoa_DePara_Banco' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_08_Pessoa_DePara_Banco];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Produto_DePara_Unidade' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_01_Produto_DePara_Unidade];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_02_Produto_DePara_TipoProduto' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_02_Produto_DePara_TipoProduto];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_03_Produto_DePara_GrupoLucratividade' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_03_Produto_DePara_GrupoLucratividade];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_04_Produto_DePara_GrupoProduto' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_04_Produto_DePara_GrupoProduto];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_05_Produto_DePara_Procedencia' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_05_Produto_DePara_Procedencia];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_06_Produto_DePara_TabelaPreco' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_06_Produto_DePara_TabelaPreco];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_ProdutoEstoque_DePara_Estoque' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_01_ProdutoEstoque_DePara_Estoque];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Veiculo_DePara_ModeloVeiculo' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_01_Veiculo_DePara_ModeloVeiculo];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_02_Veiculo_DePara_CorExterna' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_02_Veiculo_DePara_CorExterna];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_03_Veiculo_DePara_CorInterna' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_03_Veiculo_DePara_CorInterna];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_04_Veiculo_DePara_VeiculoAno' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_04_Veiculo_DePara_VeiculoAno];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_05_Veiculo_DePara_Estado' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_05_Veiculo_DePara_Estado];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_06_Veiculo_DePara_Municipio' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_06_Veiculo_DePara_Municipio];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_07_Veiculo_DePara_Marca' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_07_Veiculo_DePara_Marca];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Financeiro_DePara_AgenteCobrador' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_01_Financeiro_DePara_AgenteCobrador];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_02_Financeiro_DePara_ContaGerencial' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_02_Financeiro_DePara_ContaGerencial];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_03_Financeiro_DePara_TipoTitulo' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_03_Financeiro_DePara_TipoTitulo];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_04_Financeiro_DePara_Departamento' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_04_Financeiro_DePara_Departamento];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_05_Financeiro_DePara_NaturezaOperacao' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_05_Financeiro_DePara_NaturezaOperacao];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_06_Financeiro_DePara_Banco' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_06_Financeiro_DePara_Banco];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Adiantamento_DePara_TipoFichaRazao' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_01_Adiantamento_DePara_TipoFichaRazao];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_MovimentoEstoque_DePara_NaturezaOperacao' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_01_MovimentoEstoque_DePara_NaturezaOperacao];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_02_MovimentoEstoque_DePara_Estoque' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_02_MovimentoEstoque_DePara_Estoque];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_03_MovimentoEstoque_DePara_Departamento' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_03_MovimentoEstoque_DePara_Departamento];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Fseg_DePara_TipoOS' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_01_Fseg_DePara_TipoOS];
GO
-- <<< FIM: 00_drop_procedures.sql
GO


-- >>> INICIO: up_01_Pessoa_DePara_SegmentoMercado.sql
-- =============================================================================
-- Layout/trigger: forn_cli (pos-importacao)
-- Destino : SegmentoMercado_DePara
-- Procedure: up_01_Pessoa_DePara_SegmentoMercado
-- Params: @BancoDadosGX, @BancoWF
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Pessoa_DePara_SegmentoMercado' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_01_Pessoa_DePara_SegmentoMercado;
GO

CREATE PROCEDURE dbo.up_01_Pessoa_DePara_SegmentoMercado
	@BancoDadosGX		VARCHAR(MAX),
	@BancoWF			VARCHAR(MAX)

AS

DECLARE @CMD NVARCHAR(MAX)

-- ==========================================================================

IF ( NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX) )
	BEGIN 
		PRINT 'O < '+ @BancoDadosGX +' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
		RETURN
	END

-- ==========================================================================================

--TRUNCATE TABLE dbo.SegmentoMercado_DePara

SELECT @CMD='

PRINT ''=========================================================================================='' 
PRINT '' 01 - GERA SegmentoMercado_DePara '' 
PRINT ''==========================================================================================''
	
	INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.SegmentoMercado_DePara
		(segm_cd,
		 segm_ds,
		 SegmentoMercado_Codigo,
		 SegmentoMercado_Descricao)
	
	SELECT DISTINCT
		segm_cd						= '''', 
		segm_ds						= ISNULL(a.SEGMENTO_OFICINA,''''),
		SegmentoMercado_Codigo		= ''S/DePara'',
		SegmentoMercado_Descricao	= ''S/DePara''
	FROM 
		' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_MG a
	WHERE 
		NOT EXISTS (
			SELECT 1 
			FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.SegmentoMercado_DePara c 
			WHERE ISNULL(a.SEGMENTO_OFICINA, '''') = ISNULL(c.segm_ds, '''')
		)
		AND a.Flag = 1

	UNION

	SELECT DISTINCT
		segm_cd						= '''',
		segm_ds						= ISNULL(a.SEGMENTO_BALCAO,''''),
		SegmentoMercado_Codigo		= ''S/DePara'',
		SegmentoMercado_Descricao	= ''S/DePara''
	FROM 
		' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_MG a
	WHERE 
		NOT EXISTS (
			SELECT 1 
			FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.SegmentoMercado_DePara c 
			WHERE ISNULL(a.SEGMENTO_BALCAO, '''') = ISNULL(c.segm_ds, '''')
		)
		AND a.Flag = 1

	UNION

	SELECT DISTINCT
		segm_cd						= '''',
		segm_ds						= ISNULL(a.SEGMENTO_VENDAS,''''),
		SegmentoMercado_Codigo		= ''S/DePara'',
		SegmentoMercado_Descricao	= ''S/DePara''
	FROM 
		' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_MG a
	WHERE 
		NOT EXISTS (
			SELECT 1 
			FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.SegmentoMercado_DePara c 
			WHERE ISNULL(a.SEGMENTO_VENDAS, '''') = ISNULL(c.segm_ds, '''')
		)
		AND a.Flag = 1

'

EXEC sp_executesql @CMD

SELECT @CMD='

PRINT ''=========================================================================================='' 
PRINT '' 02 - Aplica POR DESCRIÇÃO - SegmentoMercado_DePara  '' 
PRINT ''==========================================================================================''

	UPDATE a 
	SET
		a.SegmentoMercado_Codigo		=	b.SegmentoMercado_Codigo,
		a.SegmentoMercado_Descricao		=	b.SegmentoMercado_Descricao
	FROM 
		' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.SegmentoMercado_DePara a
	INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.SegmentoMercado b ON b.SegmentoMercado_Descricao = a.segm_ds
	WHERE
		a.SegmentoMercado_Codigo	= ''S/DePara''

'

EXEC sp_executesql @CMD

GO
-- <<< FIM: up_01_Pessoa_DePara_SegmentoMercado.sql
GO


-- >>> INICIO: up_02_Pessoa_DePara_Escolaridade.sql
-- =============================================================================
-- Layout/trigger: forn_cli (pos-importacao)
-- Destino : Escolaridade_DePara
-- Procedure: up_02_Pessoa_DePara_Escolaridade
-- Params: @BancoDadosGX, @BancoWF
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_02_Pessoa_DePara_Escolaridade' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_02_Pessoa_DePara_Escolaridade;
GO

CREATE PROCEDURE dbo.up_02_Pessoa_DePara_Escolaridade
	@BancoDadosGX		VARCHAR(MAX),
	@BancoWF			VARCHAR(MAX)

AS

DECLARE @CMD NVARCHAR(MAX)

-- ==========================================================================

IF ( NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX) )
	BEGIN 
		PRINT 'O < '+ @BancoDadosGX +' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
		RETURN
	END

-- ==========================================================================================

--TRUNCATE TABLE dbo.Escolaridade_DePara

SELECT @CMD='

PRINT ''=========================================================================================='' 
PRINT '' 01 - GERA Escolaridade_DePara '' 
PRINT ''==========================================================================================''
	
	INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Escolaridade_DePara
		(escola_cd,
		 escola_ds,
		 Escolaridade_Codigo,
		 Escolaridade_Descricao)
	SELECT DISTINCT
		escola_cd				= ISNULL(a.ESCOLARIDADE_CODIGO, ''''),
		escola_ds				= ISNULL(a.ESCOLARIDADE_DESCRICAO, ''''),
		Escolaridade_Codigo		= ''S/DePara'',
		Escolaridade_Descricao	= ''S/DePara''
	FROM 
		' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_MG a
	WHERE 
		a.TIPO = ''F'' AND
		a.ESCOLARIDADE_CODIGO IS NOT NULL AND 
		a.Flag = 1 AND
		NOT EXISTS (
			SELECT 1 
			FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Escolaridade_DePara b
			WHERE ISNULL(a.ESCOLARIDADE_CODIGO, '''') = ISNULL(b.escola_cd, '''')
		)

'

EXEC sp_executesql @CMD

SELECT @CMD='

PRINT ''=========================================================================================='' 
PRINT '' 02 - Aplica POR DESCRIÇÃO - Escolaridade_DePara '' 
PRINT ''==========================================================================================''

	UPDATE a 
	SET
		a.Escolaridade_Codigo		=	b.Escolaridade_Codigo,
		a.Escolaridade_Descricao	=	b.Escolaridade_Descricao
	FROM 
		' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Escolaridade_DePara a
	INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Escolaridade b ON b.Escolaridade_Descricao = a.escola_ds
	WHERE
		a.Escolaridade_Codigo	= ''S/DePara''

'

EXEC sp_executesql @CMD

GO
-- <<< FIM: up_02_Pessoa_DePara_Escolaridade.sql
GO


-- >>> INICIO: up_03_Pessoa_DePara_Profissao.sql
-- =============================================================================
-- Layout/trigger: forn_cli (pos-importacao)
-- Destino : Profissao_DePara
-- Procedure: up_03_Pessoa_DePara_Profissao
-- Params: @BancoDadosGX, @BancoWF
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_03_Pessoa_DePara_Profissao' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_03_Pessoa_DePara_Profissao;
GO

CREATE PROCEDURE dbo.up_03_Pessoa_DePara_Profissao
	@BancoDadosGX		VARCHAR(MAX),
	@BancoWF			VARCHAR(MAX)

AS

DECLARE @CMD NVARCHAR(MAX)

-- ==========================================================================

IF ( NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX) )
	BEGIN 
		PRINT 'O < '+ @BancoDadosGX +' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
		RETURN
	END

-- ==========================================================================================

--TRUNCATE TABLE dbo.Profissao_DePara

SELECT @CMD='

PRINT ''=========================================================================================='' 
PRINT '' 01 - GERA Profissao_DePara '' 
PRINT ''==========================================================================================''
	
	INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Profissao_DePara
		(prof_cd,
		 prof_ds,
		 Profissao_Codigo,
		 Profissao_Descricao)
	SELECT DISTINCT
		prof_cd				= ISNULL(a.PROFISSAO_CODIGO, ''''),
		prof_ds				= ISNULL(a.PROFISSAO_DESCRICAO, ''''),
		Profissao_Codigo	= ''S/DePara'',
		Profissao_Descricao	= ''S/DePara''
	FROM 
		' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_MG a
	WHERE 
		a.TIPO = ''F'' AND
		a.PROFISSAO_CODIGO IS NOT NULL AND 
		a.Flag = 1 AND
		NOT EXISTS (
			SELECT 1 
			FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Profissao_DePara b
			WHERE ISNULL(a.PROFISSAO_CODIGO, '''') = ISNULL(b.prof_cd, '''')
		)

'

EXEC sp_executesql @CMD

SELECT @CMD='

PRINT ''=========================================================================================='' 
PRINT '' 02 - Aplica POR DESCRIÇÃO - Profissao_DePara '' 
PRINT ''==========================================================================================''

	UPDATE a 
	SET
		a.Profissao_Codigo		=	b.Profissao_Codigo,
		a.Profissao_Descricao	=	b.Profissao_Descricao
	FROM 
		' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Profissao_DePara a
	INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Profissao b ON b.Profissao_Descricao = a.prof_ds COLLATE Latin1_General_CI_AI
	WHERE
		a.Profissao_Codigo	= ''S/DePara''

'

EXEC sp_executesql @CMD

GO
-- <<< FIM: up_03_Pessoa_DePara_Profissao.sql
GO


-- >>> INICIO: up_04_Pessoa_DePara_EstadoCivil.sql
-- =============================================================================
-- Layout/trigger: forn_cli (pos-importacao)
-- Destino : EstadoCivil_DePara
-- Procedure: up_04_Pessoa_DePara_EstadoCivil
-- Params: @BancoDadosGX, @BancoWF
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_04_Pessoa_DePara_EstadoCivil' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_04_Pessoa_DePara_EstadoCivil;
GO

CREATE PROCEDURE dbo.up_04_Pessoa_DePara_EstadoCivil
	@BancoDadosGX		VARCHAR(MAX),
	@BancoWF			VARCHAR(MAX)

AS

DECLARE @CMD NVARCHAR(MAX)

-- ==========================================================================

IF ( NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX) )
	BEGIN 
		PRINT 'O < '+ @BancoDadosGX +' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
		RETURN
	END

-- ==========================================================================================

--TRUNCATE TABLE dbo.EstadoCivil_DePara

SELECT @CMD='

PRINT ''=========================================================================================='' 
PRINT '' 01 - GERA EstadoCivil_DePara '' 
PRINT ''==========================================================================================''
	
	INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.EstadoCivil_DePara
		(estcivil_cd,
		 estcivil_ds,
		 EstadoCivil_Codigo,
		 EstadoCivil_Descricao)
	SELECT DISTINCT
		estcivil_cd            = ISNULL(a.ESTADO_CIVIL_CODIGO, ''''),
		estcivil_ds            = ISNULL(a.ESTADO_CIVIL_DESCRICAO, ''''),
		EstadoCivil_Codigo     = ''S/DePara'',
		EstadoCivil_Descricao  = ''S/DePara''
	FROM 
		' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_MG a
	WHERE 
		a.TIPO = ''F'' AND
		a.ESTADO_CIVIL_CODIGO IS NOT NULL AND 
		a.Flag = 1 AND
		NOT EXISTS (
			SELECT 1 
			FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.EstadoCivil_DePara b
			WHERE ISNULL(a.ESTADO_CIVIL_CODIGO, '''') = ISNULL(b.estcivil_cd, '''')
		)

'

EXEC sp_executesql @CMD

SELECT @CMD='

PRINT ''=========================================================================================='' 
PRINT '' 02 - Aplica POR DESCRIÇÃO - EstadoCivil_DePara '' 
PRINT ''==========================================================================================''

	UPDATE a 
	SET
		a.EstadoCivil_Codigo = b.EstadoCivil_Codigo,
		a.EstadoCivil_Descricao = b.EstadoCivil_Descricao
	FROM 
		' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.EstadoCivil_DePara a
	INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.EstadoCivil b ON REPLACE(b.EstadoCivil_Descricao,''(A)'','''') = a.estcivil_ds COLLATE Latin1_General_CI_AI
	WHERE
		a.EstadoCivil_Codigo	= ''S/DePara''
'

EXEC sp_executesql @CMD

GO
-- <<< FIM: up_04_Pessoa_DePara_EstadoCivil.sql
GO


-- >>> INICIO: up_05_Pessoa_DePara_Municipio.sql
-- =============================================================================
-- Layout/trigger: forn_cli_endereco (pos-importacao)
-- Destino : Municipio_DePara
-- Procedure: up_05_Pessoa_DePara_Municipio
-- Params: @BancoDadosGX, @BancoWF
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_05_Pessoa_DePara_Municipio' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_05_Pessoa_DePara_Municipio;
GO

CREATE PROCEDURE dbo.up_05_Pessoa_DePara_Municipio
	@BancoDadosGX		VARCHAR(MAX),
	@BancoWF			VARCHAR(MAX)

AS

DECLARE @CMD NVARCHAR(MAX)

-- ==========================================================================

IF ( NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX) )
	BEGIN 
		PRINT 'O < '+ @BancoDadosGX +' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
		RETURN
	END

-- ==========================================================================================

--TRUNCATE TABLE dbo.Municipio_DePara

SELECT @CMD='

PRINT ''=========================================================================================='' 
PRINT '' 01 - GERA Municipio_DePara '' 
PRINT ''==========================================================================================''
	
	INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Municipio_DePara
		(cg_cidade
		,Municipio_IBGE
		,uf_cd
		,Municipio_Codigo
		,Municipio_Nome
		,Estado_Codigo
		,Tabela)
	SELECT DISTINCT
		cg_cidade			= ISNULL(RTRIM(LTRIM(UPPER(a.CIDADE))) COLLATE SQL_Latin1_General_CP1253_CI_AI,'''')
		,Municipio_IBGE		= ISNULL(RTRIM(LTRIM(a.COD_IBGE)),'''')
		,uf_cd				= ISNULL(RTRIM(LTRIM(a.ESTADO)),'''')
		,Municipio_Codigo	= ''S/DePara''
		,Municipio_Nome		= ''S/DePara''
		,Estado_Codigo		= ''S/DePara''
		,Tabela				= ''Pessoa''
	FROM 
		' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaEndereco_MG a
	WHERE 
		a.Flag = 1 AND
		NOT EXISTS (
			SELECT 1 
			FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Municipio_DePara b
			WHERE
				RTRIM( LTRIM( ISNULL(a.CIDADE,'''' ) ) )	= RTRIM( LTRIM( ISNULL(b.cg_cidade,'''' ) ) )	COLLATE SQL_Latin1_General_CP1253_CI_AI
				AND	RTRIM( LTRIM( ISNULL(a.ESTADO,'''' ) ) )	= ISNULL(b.uf_cd,'''')						COLLATE SQL_Latin1_General_CP1253_CI_AI
				AND	RTRIM( LTRIM( ISNULL(a.COD_IBGE,'''' ) ) )	= ISNULL(b.Municipio_IBGE,'''')				COLLATE SQL_Latin1_General_CP1253_CI_AI
		)
'

EXEC sp_executesql @CMD

SELECT @CMD='

PRINT ''=========================================================================================='' 
PRINT '' 02 - Aplica POR COD_IBGE/cg_cidade/uf_cd - Municipio_DePara '' 
PRINT ''==========================================================================================''
	
	UPDATE a 
	SET
		a.Municipio_Codigo	= b.Municipio_Codigo
		,a.Municipio_Nome	= b.Municipio_Nome
		,a.Estado_Codigo	= b.Estado_Codigo
	FROM 
		'+LTRIM(RTRIM(@BancoDadosGX))+'.dbo.Municipio_DePara a
	INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Municipio b ON a.Municipio_IBGE	=	RTRIM( LTRIM(b.Municipio_IBGE) )	COLLATE SQL_Latin1_General_CP1253_CI_AI 
													AND	RTRIM( LTRIM(a.cg_cidade) )	=	RTRIM( LTRIM(b.Municipio_Nome) )	COLLATE SQL_Latin1_General_CP1253_CI_AI
	     											AND	RTRIM( LTRIM(a.uf_cd) )		=	b.Estado_Codigo						COLLATE SQL_Latin1_General_CP1253_CI_AI
	WHERE
		ISNUMERIC(a.Municipio_IBGE) = 1		AND 
		ISNUMERIC(a.Municipio_Codigo) = 0	

'

EXEC sp_executesql @CMD

SELECT @CMD='

PRINT ''=========================================================================================='' 
PRINT '' 03 - Aplica POR COD_IBGE/uf_cd - Municipio_DePara '' 
PRINT ''==========================================================================================''
	
	UPDATE a 
	SET
		a.Municipio_Codigo	= b.Municipio_Codigo
		,a.Municipio_Nome	= b.Municipio_Nome
		,a.Estado_Codigo	= b.Estado_Codigo
	FROM 
		'+LTRIM(RTRIM(@BancoDadosGX))+'.dbo.Municipio_DePara a
	INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Municipio b ON a.Municipio_IBGE	=	RTRIM( LTRIM(b.Municipio_IBGE) )	COLLATE SQL_Latin1_General_CP1253_CI_AI 
	     											AND	RTRIM( LTRIM(a.uf_cd) )		=	b.Estado_Codigo						COLLATE SQL_Latin1_General_CP1253_CI_AI
	WHERE
		ISNUMERIC(a.Municipio_IBGE) = 1		AND 
		ISNUMERIC(a.Municipio_Codigo) = 0	

'

EXEC sp_executesql @CMD

SELECT @CMD='

PRINT ''=========================================================================================='' 
PRINT '' 04 - Aplica POR COD_IBGE - Municipio_DePara '' 
PRINT ''==========================================================================================''
	
	UPDATE a 
	SET
		a.Municipio_Codigo	= b.Municipio_Codigo
		,a.Municipio_Nome	= b.Municipio_Nome
		,a.Estado_Codigo	= b.Estado_Codigo
	FROM 
		'+LTRIM(RTRIM(@BancoDadosGX))+'.dbo.Municipio_DePara a
	INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Municipio b ON a.Municipio_IBGE	=	RTRIM( LTRIM(b.Municipio_IBGE) )	COLLATE SQL_Latin1_General_CP1253_CI_AI 
	WHERE
		ISNUMERIC(a.Municipio_IBGE) = 1		AND 
		ISNUMERIC(a.Municipio_Codigo) = 0	

'

EXEC sp_executesql @CMD

SELECT @CMD='

PRINT ''=========================================================================================='' 
PRINT '' 04 - Aplica POR cg_cidade/uf_cd - Municipio_DePara '' 
PRINT ''==========================================================================================''
	
	UPDATE a 
	SET
		a.Municipio_Codigo	= b.Municipio_Codigo
		,a.Municipio_Nome	= b.Municipio_Nome
		,a.Estado_Codigo	= b.Estado_Codigo
	FROM 
		'+LTRIM(RTRIM(@BancoDadosGX))+'.dbo.Municipio_DePara a
	INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Municipio b ON RTRIM( LTRIM(a.cg_cidade) )	=	RTRIM( LTRIM(b.Municipio_Nome) )	COLLATE SQL_Latin1_General_CP1253_CI_AI
	     														AND	RTRIM( LTRIM(a.uf_cd) )		=	b.Estado_Codigo						COLLATE SQL_Latin1_General_CP1253_CI_AI
	WHERE
		ISNUMERIC(a.Municipio_Codigo) = 0	

'

EXEC sp_executesql @CMD

GO
-- <<< FIM: up_05_Pessoa_DePara_Municipio.sql
GO


-- >>> INICIO: up_06_Pessoa_DePara_TipoLogradouro.sql
-- =============================================================================
-- Layout/trigger: forn_cli_endereco (pos-importacao)
-- Destino : TipoLogradouro_DePara
-- Procedure: up_06_Pessoa_DePara_TipoLogradouro
-- Params: @BancoDadosGX, @BancoWF
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_06_Pessoa_DePara_TipoLogradouro' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_06_Pessoa_DePara_TipoLogradouro;
GO

CREATE PROCEDURE dbo.up_06_Pessoa_DePara_TipoLogradouro
	@BancoDadosGX		VARCHAR(MAX),
	@BancoWF			VARCHAR(MAX)

AS

DECLARE @CMD NVARCHAR(MAX)

-- ==========================================================================

IF ( NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX) )
	BEGIN 
		PRINT 'O < '+ @BancoDadosGX +' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
		RETURN
	END

-- ==========================================================================================

--TRUNCATE TABLE dbo.TipoLogradouro_DePara

SELECT @CMD='

PRINT ''=========================================================================================='' 
PRINT '' 01 - GERA TipoLogradouro_DePara '' 
PRINT ''==========================================================================================''
	
	INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoLogradouro_DePara
		(logradouro_sigla
		,logradouro_nm
		,TipoLogradouro_Codigo
		,TipoLogradouro_Sigla
		,TipoLogradouro_Descricao
		,Tabela)
	SELECT DISTINCT
		logradouro_sigla			= ''''
		,logradouro_nm				= ISNULL( RTRIM( LTRIM(a.TIPO_LOGRADOURO)),'''')
		,TipoLogradouro_Codigo		= ''S/DePara''
		,TipoLogradouro_Sigla		= ''S/DePara''
		,TipoLogradouro_Descricao	= ''S/DePara''
		,Tabela						= ''Pessoa''
	FROM 
		' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaEndereco_MG a
	WHERE 
		a.Flag = 1 AND
		NOT EXISTS (
			SELECT 1 
			FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoLogradouro_DePara b
			WHERE
				ISNULL(a.TIPO_LOGRADOURO,'''') = ISNULL(b.logradouro_nm,'''') COLLATE Latin1_General_CI_AI
		) 

'

EXEC sp_executesql @CMD

SELECT @CMD='

PRINT ''=========================================================================================='' 
PRINT '' 02 - Aplica POR DESCRIÇÃO - TipoLogradouro_DePara '' 
PRINT ''==========================================================================================''

	UPDATE a 
	SET
		a.TipoLogradouro_Codigo = b.TipoLogradouro_Codigo,
		a.TipoLogradouro_Sigla	= b.TipoLogradouro_Sigla,
		a.TipoLogradouro_Descricao = b.TipoLogradouro_Descricao
	FROM 
		' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoLogradouro_DePara a
	INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.TipoLogradouro b ON b.TipoLogradouro_Descricao = a.logradouro_nm
	WHERE
		a.TipoLogradouro_Codigo	= ''S/DePara''
'

EXEC sp_executesql @CMD

GO
-- <<< FIM: up_06_Pessoa_DePara_TipoLogradouro.sql
GO


-- >>> INICIO: up_07_Pessoa_DePara_Estado_Pais.sql
-- =============================================================================
-- Layout/trigger: forn_cli_endereco (pos-importacao)
-- Destino : Estado_DePara, Pais_DePara
-- Procedure: up_07_Pessoa_DePara_Estado_Pais
-- Params: @BancoDadosGX, @BancoWF
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_07_Pessoa_DePara_Estado_Pais' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_07_Pessoa_DePara_Estado_Pais;
GO

CREATE PROCEDURE dbo.up_07_Pessoa_DePara_Estado_Pais
	@BancoDadosGX		VARCHAR(MAX),
	@BancoWF			VARCHAR(MAX)

AS

DECLARE @CMD NVARCHAR(MAX)

-- ==========================================================================

IF ( NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX) )
	BEGIN 
		PRINT 'O < '+ @BancoDadosGX +' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
		RETURN
	END

-- ==========================================================================================

--TRUNCATE TABLE .dbo.Estado_DePara
--TRUNCATE TABLE .dbo.Pais_DePara

SELECT @CMD='

PRINT ''=========================================================================================='' 
PRINT '' 01 - GERA Estado_DePara '' 
PRINT ''==========================================================================================''
	

	INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Estado_DePara
		(uf_cd
		,uf_nm
		,Estado_Codigo
		,Estado_Nome
		,Tabela)
	SELECT DISTINCT  
		uf_cd			= ISNULL( RTRIM( LTRIM(a.ESTADO)),'''')
		,uf_nm			= ''''
		,Estado_Codigo	= ''S/DePara''
		,Estado_Nome	= ''S/DePara''
		,Tabela			= ''Pessoa''
	FROM 
		' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaEndereco_MG a	
	WHERE 
		a.Flag = 1 AND
		NOT EXISTS(
			SELECT 1 
			FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Estado_DePara b 
			WHERE b.UF_CD = ISNULL( RTRIM( LTRIM(a.ESTADO)),'''')  COLLATE Latin1_General_CI_AI
		)
	ORDER BY 1

	IF NOT EXISTS ( SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col, ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj 
				WHERE col.object_id = obj.object_id and col.name = ''Pais_Codigo'' AND obj.name = ''Estado_DePara'')
	BEGIN
		ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Estado_DePara ADD Pais_Codigo smallint
	END

PRINT ''=========================================================================================='' 
PRINT '' 01.1 - GERA Pais_DePara '' 
PRINT ''==========================================================================================''

	INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pais_DePara
		(pais_cd
		,pais_ds
		,Pais_Codigo
		,Pais_Nome)
	SELECT DISTINCT  
		pais_cd			= ''''
		,pais_ds		= ISNULL( RTRIM( LTRIM(a.PAIS)),'''')
		,Pais_Codigo	= ''S/DePara''
		,Pais_Nome		= ''S/DePara''
	FROM 
		' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaEndereco_MG a	
	WHERE 
		a.Flag = 1 AND
		NOT EXISTS(
			SELECT 1 
			FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pais_DePara b 
			WHERE b.pais_ds = ISNULL( RTRIM( LTRIM(a.PAIS)),'''')  COLLATE Latin1_General_CI_AI
		)
	ORDER BY 1

'

EXEC sp_executesql @CMD

SELECT @CMD='

PRINT ''=========================================================================================='' 
PRINT '' 02 - Aplica POR DESCRIÇÃO - Estado_DePara '' 
PRINT ''==========================================================================================''

	UPDATE a 
	SET
		a.Estado_Codigo = b.Estado_Codigo,
		a.Estado_Nome	= b.Estado_Nome
	FROM 
		' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Estado_DePara a
	INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Estado b ON b.Estado_Codigo = a.uf_cd
	WHERE
		a.Estado_Codigo	= ''S/DePara''

PRINT ''=========================================================================================='' 
PRINT '' 02.1 - Aplica POR DESCRIÇÃO - Pais_DePara '' 
PRINT ''==========================================================================================''

	UPDATE a 
	SET
		a.Pais_Codigo = b.Pais_Codigo,
		a.Pais_Nome	= b.Pais_Nome
	FROM 
		' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pais_DePara a
	INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Pais b ON b.Pais_Nome = a.pais_ds
	WHERE
		a.Pais_Codigo	= ''S/DePara''

	UPDATE a 
	SET 
		a.Pais_Codigo = b.Pais_Codigo
	FROM 
		' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Estado_DePara a
	INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Estado b on a.Estado_Codigo = b.Estado_Codigo COLLATE database_default
	WHERE
		a.Pais_Codigo IS NULL

'

EXEC sp_executesql @CMD

GO
-- <<< FIM: up_07_Pessoa_DePara_Estado_Pais.sql
GO


-- >>> INICIO: up_08_Pessoa_DePara_Banco.sql
-- =============================================================================
-- Layout/trigger: forn_cli_dados_bancarios (pos-importacao)
-- Destino : Banco_DePara
-- Procedure: up_08_Pessoa_DePara_Banco
-- Params: @BancoDadosGX, @BancoWF
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_08_Pessoa_DePara_Banco' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_08_Pessoa_DePara_Banco;
GO

CREATE PROCEDURE dbo.up_08_Pessoa_DePara_Banco
	@BancoDadosGX		VARCHAR(MAX),
	@BancoWF			VARCHAR(MAX)

AS

DECLARE @CMD NVARCHAR(MAX)

-- ==========================================================================

IF ( NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX) )
	BEGIN 
		PRINT 'O < '+ @BancoDadosGX +' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
		RETURN
	END

-- ==========================================================================================

--TRUNCATE TABLE .dbo.Banco_DePara

SELECT @CMD='

PRINT ''=========================================================================================='' 
PRINT '' 01 - GERA Banco_DePara '' 
PRINT ''==========================================================================================''	

	IF EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.tables WHERE name = ''PessoaBanco_MG'')
	BEGIN 	
		INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Banco_DePara
			(ban_cd
			,ban_ds
			,Banco_Codigo
			,Banco_Sigla
			,Banco_Descricao)
		SELECT DISTINCT
			ban_cd				= ISNULL( RTRIM( LTRIM(a.PessoaBanco_BancoCod)),'''')
			,ban_ds				= ''''
			,Banco_Codigo		= ''S/DePara''
			,Banco_Sigla		= ''S/DePara''
			,Banco_Descricao	= ''S/DePara''

		FROM 
			' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaBanco_MG a	
		WHERE 
			a.Flag = 1 AND
			NOT EXISTS(
				SELECT 1 
				FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Banco_DePara b 
				WHERE b.ban_cd = ISNULL( RTRIM( LTRIM(a.PessoaBanco_BancoCod)),'''')  COLLATE Latin1_General_CI_AI
			)
	END
	ELSE
		PRINT '' TABELA PessoaBanco_MG NÃO ENCONTRADA! ''

'

EXEC sp_executesql @CMD

SELECT @CMD='

PRINT ''=========================================================================================='' 
PRINT '' 02 - Aplica POR DESCRIÇÃO - Banco_DePara '' 
PRINT ''==========================================================================================''

	IF EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.tables WHERE name = ''PessoaBanco_MG'')
	BEGIN 	

		UPDATE a 
		SET
			a.Banco_Codigo		= b.Banco_Codigo,
			a.Banco_Sigla		= b.Banco_Sigla,
			a.Banco_Descricao	= b.Banco_Descricao
		FROM 
			' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Banco_DePara a
		INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Banco b ON b.Banco_Codigo = a.Banco_Codigo
		WHERE
			a.Banco_Codigo	= ''S/DePara''
	END
	ELSE
		PRINT '' Não foi possível ATUALIZAR a descrição na Banco_DePara, pois a tabela PessoaBanco_MG NÃO EXISTE! ''

'

EXEC sp_executesql @CMD

GO
-- <<< FIM: up_08_Pessoa_DePara_Banco.sql
GO


-- >>> INICIO: up_01_Produto_DePara_Unidade.sql
-- =============================================================================
-- Layout: 7 Produto De/Para
-- Tabela : Unidade_DePara
-- Procedure: up_01_Produto_DePara_Unidade (@BancoDadosGX, @BancoWF)
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Produto_DePara_Unidade' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_01_Produto_DePara_Unidade;
GO

CREATE PROCEDURE dbo.up_01_Produto_DePara_Unidade
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF (NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX))
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

SELECT @CMD = '

PRINT ''=========================================================================================='' 
PRINT '' 01 - GERA Unidade_DePara '' 
PRINT ''==========================================================================================''

    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Unidade_DePara
        (pdund_cd, pdund_ds, Unidade_Codigo, Unidade_Descricao)
    SELECT DISTINCT
        pdund_cd = ISNULL(a.UNIDADE_PRODUTO_CODIGO, ''''),
        pdund_ds = ISNULL(a.UNIDADE_PRODUTO_DESCRICAO, ''''),
        Unidade_Codigo = ''S/DePara'',
        Unidade_Descricao = ''S/DePara''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG a
    WHERE
        a.Flag = 1 AND
        NOT EXISTS (
            SELECT 1
            FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Unidade_DePara b
            WHERE ISNULL(a.UNIDADE_PRODUTO_CODIGO, '''') = ISNULL(b.pdund_cd, '''')
        )
'
EXEC sp_executesql @CMD

SELECT @CMD = '

PRINT ''=========================================================================================='' 
PRINT '' 02 - Aplica POR DESCRICAO - Unidade_DePara '' 
PRINT ''==========================================================================================''

    UPDATE a
    SET
        a.Unidade_Codigo = b.Unidade_Codigo,
        a.Unidade_Descricao = b.Unidade_Descricao
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Unidade_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Unidade b ON b.Unidade_Descricao = a.pdund_ds COLLATE Latin1_General_CI_AI
    WHERE
        a.Unidade_Codigo = ''S/DePara''
'
EXEC sp_executesql @CMD
GO
-- <<< FIM: up_01_Produto_DePara_Unidade.sql
GO


-- >>> INICIO: up_02_Produto_DePara_TipoProduto.sql
-- =============================================================================
-- Layout: 7 Produto De/Para
-- Tabela : TipoProduto_DePara
-- Procedure: up_02_Produto_DePara_TipoProduto (@BancoDadosGX, @BancoWF)
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_02_Produto_DePara_TipoProduto' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_02_Produto_DePara_TipoProduto;
GO

CREATE PROCEDURE dbo.up_02_Produto_DePara_TipoProduto
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF (NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX))
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

SELECT @CMD = '

PRINT ''=========================================================================================='' 
PRINT '' 01 - GERA TipoProduto_DePara '' 
PRINT ''==========================================================================================''

    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoProduto_DePara
        (tpd_cd, tpd_ds, TipoProduto_Codigo, TipoProduto_Descricao, TipoProduto_GrupoContabilCod)
    SELECT DISTINCT
        tpd_cd = ISNULL(a.TIPO_PRODUTO_CODIGO, ''''),
        tpd_ds = ISNULL(a.TIPO_PRODUTO_DESCRICAO, ''''),
        TipoProduto_Codigo = ''S/DePara'',
        TipoProduto_Descricao = ''S/DePara'',
        TipoProduto_GrupoContabilCod = ''S/DePara''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG a
    WHERE
        a.Flag = 1 AND
        NOT EXISTS (
            SELECT 1
            FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoProduto_DePara b
            WHERE ISNULL(a.TIPO_PRODUTO_CODIGO, '''') = ISNULL(b.tpd_cd, '''')
        )
'
EXEC sp_executesql @CMD

SELECT @CMD = '

PRINT ''=========================================================================================='' 
PRINT '' 02 - Aplica POR DESCRICAO - TipoProduto_DePara '' 
PRINT ''==========================================================================================''

    UPDATE a
    SET
        a.TipoProduto_Codigo = b.TipoProduto_Codigo,
        a.TipoProduto_Descricao = b.TipoProduto_Descricao,
        a.TipoProduto_GrupoContabilCod = b.TipoProduto_GrupoContabilCod
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoProduto_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.TipoProduto b ON b.TipoProduto_Descricao = a.tpd_ds COLLATE Latin1_General_CI_AI
    WHERE
        a.TipoProduto_Codigo = ''S/DePara''
'
EXEC sp_executesql @CMD
GO
-- <<< FIM: up_02_Produto_DePara_TipoProduto.sql
GO


-- >>> INICIO: up_03_Produto_DePara_GrupoLucratividade.sql
-- =============================================================================
-- Layout: 7 Produto De/Para
-- Tabela : GrupoLucratividade_DePara
-- Procedure: up_03_Produto_DePara_GrupoLucratividade (@BancoDadosGX, @BancoWF)
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_03_Produto_DePara_GrupoLucratividade' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_03_Produto_DePara_GrupoLucratividade;
GO

CREATE PROCEDURE dbo.up_03_Produto_DePara_GrupoLucratividade
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF (NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX))
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

SELECT @CMD = '

PRINT ''=========================================================================================='' 
PRINT '' 01 - GERA GrupoLucratividade_DePara '' 
PRINT ''==========================================================================================''

    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.GrupoLucratividade_DePara
        (letr_cd, letr_ds, letr_cdmont, GrupoLucratividade_Codigo, GrupoLucratividade_Descricao, GrupoLucratividade_MarcaCod, GrupoLucratividade_Letra)
    SELECT DISTINCT
        letr_cd = ISNULL(a.GRUPO_LUCRATIVIDADE_CODIGO, ''''),
        letr_ds = ISNULL(a.GRUPO_LUCRATIVIDADE_DESCRICAO, ''''),
        letr_cdmont = '''',
        GrupoLucratividade_Codigo = ''S/DePara'',
        GrupoLucratividade_Descricao = ''S/DePara'',
        GrupoLucratividade_MarcaCod = ISNULL(a.ProdutoMarca_MarcaCod, ''''),
        GrupoLucratividade_Letra = ''S/DePara''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG a
    WHERE
        a.Flag = 1 AND
        NOT EXISTS (
            SELECT 1
            FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.GrupoLucratividade_DePara b
            WHERE
                ISNULL(a.GRUPO_LUCRATIVIDADE_CODIGO, '''') = ISNULL(b.letr_cd, '''') AND
                ISNULL(a.GRUPO_LUCRATIVIDADE_DESCRICAO, '''') = ISNULL(b.letr_ds, '''') AND
                a.ProdutoMarca_MarcaCod = b.GrupoLucratividade_MarcaCod
        )
'
EXEC sp_executesql @CMD

SELECT @CMD = '

PRINT ''=========================================================================================='' 
PRINT '' 02 - Aplica POR DESCRICAO - GrupoLucratividade_DePara '' 
PRINT ''==========================================================================================''

    UPDATE a
    SET
        a.GrupoLucratividade_Codigo = b.GrupoLucratividade_Codigo,
        a.GrupoLucratividade_Descricao = b.GrupoLucratividade_Descricao,
        a.GrupoLucratividade_MarcaCod = b.GrupoLucratividade_MarcaCod,
        a.GrupoLucratividade_Letra = b.GrupoLucratividade_Letra
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.GrupoLucratividade_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.GrupoLucratividade b ON
        b.GrupoLucratividade_Letra = RTRIM(LTRIM(a.letr_cd)) COLLATE Latin1_General_CI_AI AND
        a.GrupoLucratividade_MarcaCod = b.GrupoLucratividade_MarcaCod
    WHERE
        a.GrupoLucratividade_Codigo = ''S/DePara''
'
EXEC sp_executesql @CMD
GO
-- <<< FIM: up_03_Produto_DePara_GrupoLucratividade.sql
GO


-- >>> INICIO: up_04_Produto_DePara_GrupoProduto.sql
-- =============================================================================
-- Layout: 7 Produto De/Para
-- Tabela : GrupoProduto_DePara
-- Procedure: up_04_Produto_DePara_GrupoProduto (@BancoDadosGX, @BancoWF)
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_04_Produto_DePara_GrupoProduto' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_04_Produto_DePara_GrupoProduto;
GO

CREATE PROCEDURE dbo.up_04_Produto_DePara_GrupoProduto
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF (NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX))
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

SELECT @CMD = '

PRINT ''=========================================================================================='' 
PRINT '' 01 - GERA GrupoProduto_DePara '' 
PRINT ''==========================================================================================''

    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.GrupoProduto_DePara
        (grup_cd, grup_ds, GrupoProduto_Codigo, GrupoProduto_Descricao)
    SELECT DISTINCT
        grup_cd = ISNULL(a.GRUPO_PRODUTO_CODIGO, ''''),
        grup_ds = ISNULL(a.GRUPO_PRODUTO_DESCRICAO, ''''),
        GrupoProduto_Codigo = ''S/DePara'',
        GrupoProduto_Descricao = ''S/DePara''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG a
    WHERE
        a.Flag = 1 AND
        NOT EXISTS (
            SELECT 1
            FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.GrupoProduto_DePara b
            WHERE ISNULL(a.GRUPO_PRODUTO_CODIGO, '''') = ISNULL(b.grup_cd, '''')
        )
'
EXEC sp_executesql @CMD

SELECT @CMD = '

PRINT ''=========================================================================================='' 
PRINT '' 02 - Aplica POR DESCRICAO - GrupoProduto_DePara '' 
PRINT ''==========================================================================================''

    UPDATE a
    SET
        a.GrupoProduto_Codigo = b.GrupoProduto_Codigo,
        a.GrupoProduto_Descricao = b.GrupoProduto_Descricao
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.GrupoProduto_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.GrupoProduto b ON b.GrupoProduto_Descricao = a.grup_ds COLLATE Latin1_General_CI_AI
    WHERE
        a.GrupoProduto_Codigo = ''S/DePara''
'
EXEC sp_executesql @CMD
GO
-- <<< FIM: up_04_Produto_DePara_GrupoProduto.sql
GO


-- >>> INICIO: up_05_Produto_DePara_Procedencia.sql
-- =============================================================================
-- Layout: 7 Produto De/Para
-- Tabela : Procedencia_DePara
-- Procedure: up_05_Produto_DePara_Procedencia (@BancoDadosGX, @BancoWF)
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_05_Produto_DePara_Procedencia' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_05_Produto_DePara_Procedencia;
GO

CREATE PROCEDURE dbo.up_05_Produto_DePara_Procedencia
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF (NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX))
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

SELECT @CMD = '

PRINT ''=========================================================================================='' 
PRINT '' 01 - GERA Procedencia_DePara '' 
PRINT ''==========================================================================================''

    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Procedencia_DePara
        (pro_cd, pro_ds, Procedencia_Codigo, Procedencia_Descricao)
    SELECT DISTINCT
        pro_cd = ISNULL(a.PROCEDENCIA_CODIGO, ''''),
        pro_ds = ISNULL(a.PROCEDENCIA_DESCRICAO, ''''),
        Procedencia_Codigo = ''S/DePara'',
        Procedencia_Descricao = ''S/DePara''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG a
    WHERE
        a.Flag = 1 AND
        NOT EXISTS (
            SELECT 1
            FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Procedencia_DePara b
            WHERE ISNULL(a.PROCEDENCIA_CODIGO, '''') = ISNULL(b.pro_cd, '''')
        )
'
EXEC sp_executesql @CMD

SELECT @CMD = '

PRINT ''=========================================================================================='' 
PRINT '' 02 - Aplica POR DESCRICAO - Procedencia_DePara '' 
PRINT ''==========================================================================================''

    UPDATE a
    SET
        a.Procedencia_Codigo = b.Procedencia_Codigo,
        a.Procedencia_Descricao = b.Procedencia_Descricao
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Procedencia_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Procedencia b ON b.Procedencia_Descricao = a.pro_ds COLLATE Latin1_General_CI_AI
    WHERE
        a.Procedencia_Codigo = ''S/DePara''
'
EXEC sp_executesql @CMD
GO
-- <<< FIM: up_05_Produto_DePara_Procedencia.sql
GO


-- >>> INICIO: up_06_Produto_DePara_TabelaPreco.sql
-- =============================================================================
-- Layout: 7 Produto De/Para
-- Tabela : TabelaPreco_DePara
-- Procedure: up_06_Produto_DePara_TabelaPreco (@BancoDadosGX, @BancoWF)
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_06_Produto_DePara_TabelaPreco' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_06_Produto_DePara_TabelaPreco;
GO

CREATE PROCEDURE dbo.up_06_Produto_DePara_TabelaPreco
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF (NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX))
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

SELECT @CMD = '

PRINT ''=========================================================================================='' 
PRINT '' 01 - GERA TabelaPreco_DePara '' 
PRINT ''==========================================================================================''

    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TabelaPreco_DePara
        (Empresa_Codigo, Empresa_NomeFantasia, EmpresaTabelaPreco_TabPrecoCod, EmpresaTabelaPreco_TabelaPrecoTipo, TabelaPreco_Codigo, TabelaPreco_Descricao, TabelaPreco_Tipo, banco_principal)
    SELECT DISTINCT
        Empresa_Codigo = a.Empresa_Codigo,
        Empresa_NomeFantasia = b.Empresa_NomeFantasia,
        EmpresaTabelaPreco_TabPrecoCod = a.EmpresaTabelaPreco_TabPrecoCod,
        EmpresaTabelaPreco_TabelaPrecoTipo = a.EmpresaTabelaPreco_TabelaPrecoTipo,
        TabelaPreco_Codigo = c.TabelaPreco_Codigo,
        TabelaPreco_Descricao = c.TabelaPreco_Descricao,
        TabelaPreco_Tipo = c.TabelaPreco_Tipo,
        banco_principal = b.Empresa_MarcaCod
    FROM ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.EmpresaTabelaPreco a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Empresa b ON (a.Empresa_Codigo = b.Empresa_Codigo)
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.TabelaPreco c ON (
        a.EmpresaTabelaPreco_TabPrecoCod = c.TabelaPreco_Codigo AND
        a.EmpresaTabelaPreco_TabelaPrecoTipo = c.TabelaPreco_Tipo
    )
    INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Empresa_DePara d ON (b.Empresa_Codigo = d.Empresa_Codigo)
    WHERE
        NOT EXISTS (
            SELECT 1
            FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TabelaPreco_DePara e
            WHERE
                e.Empresa_Codigo = a.Empresa_Codigo AND
                e.EmpresaTabelaPreco_TabPrecoCod = a.EmpresaTabelaPreco_TabPrecoCod
        )
'
EXEC sp_executesql @CMD
GO
-- <<< FIM: up_06_Produto_DePara_TabelaPreco.sql
GO


-- >>> INICIO: up_01_ProdutoEstoque_DePara_Estoque.sql
-- =============================================================================
-- Layout: 8 ProdutoEstoque De/Para
-- Tabela : Estoque_DePara
-- Procedure: up_01_ProdutoEstoque_DePara_Estoque (@BancoDadosGX, @BancoWF)
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_ProdutoEstoque_DePara_Estoque' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_01_ProdutoEstoque_DePara_Estoque;
GO

CREATE PROCEDURE dbo.up_01_ProdutoEstoque_DePara_Estoque
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF (NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX))
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

IF (NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoWF))
BEGIN
    PRINT 'O < ' + @BancoWF + ' > INFORMADO COMO @BancoWF NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

SELECT @CMD = '
PRINT ''==========================================================================================''
PRINT '' 01 - GERA Estoque_DePara (origem ProdutoEstoque_MG)''
PRINT ''==========================================================================================''

    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Estoque_DePara
        (est_cd, est_ds)
    SELECT DISTINCT
        est_cd = RTRIM(LTRIM(a.ESTOQUE_CODIGO)),
        est_ds = RTRIM(LTRIM(a.ESTOQUE_CODIGO))
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ProdutoEstoque_MG a
    WHERE
        a.Flag = 1
        AND RTRIM(LTRIM(ISNULL(a.ESTOQUE_CODIGO, ''''))) <> ''''
        AND NOT EXISTS (
            SELECT 1
            FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Estoque_DePara b
            WHERE b.est_cd = RTRIM(LTRIM(a.ESTOQUE_CODIGO))
        )

PRINT ''==========================================================================================''
PRINT '' 02 - ATUALIZA Estoque_Codigo / Estoque_Descricao via WF''
PRINT ''==========================================================================================''

    UPDATE a
    SET
        a.Estoque_Codigo = b.Estoque_Codigo,
        a.Estoque_Descricao = b.Estoque_Descricao,
        a.Origem = ''ProdutoEstoque''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Estoque_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Estoque b
        ON RTRIM(LTRIM(a.est_cd)) = RTRIM(LTRIM(b.Estoque_Sigla)) COLLATE database_default
'
EXEC sp_executesql @CMD
GO
-- <<< FIM: up_01_ProdutoEstoque_DePara_Estoque.sql
GO


-- >>> INICIO: up_01_Veiculo_DePara_ModeloVeiculo.sql
-- =============================================================================
-- Layout: Veiculo De/Para — ModeloVeiculo
-- Procedure: up_01_Veiculo_DePara_ModeloVeiculo (@BancoDadosGX, @BancoWF)
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Veiculo_DePara_ModeloVeiculo' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_01_Veiculo_DePara_ModeloVeiculo;
GO

CREATE PROCEDURE dbo.up_01_Veiculo_DePara_ModeloVeiculo
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX)
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

-- 1) Garante colunas extras em ModeloVeiculo_DePara em lote PRÓPRIO.
--    (ALTER ADD e o uso da coluna não podem estar no mesmo lote: o SQL Server
--     compila o lote inteiro antes de executar e acusaria "Invalid column name".)
SELECT @CMD = '
    IF NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns WHERE name = ''MARCA_CODIGO'' AND object_id = OBJECT_ID(''' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ModeloVeiculo_DePara''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ModeloVeiculo_DePara ADD MARCA_CODIGO nvarchar(510) NULL
'
EXEC sp_executesql @CMD

SELECT @CMD = '
    IF NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns WHERE name = ''CODIGO_LINHA'' AND object_id = OBJECT_ID(''' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ModeloVeiculo_DePara''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ModeloVeiculo_DePara ADD CODIGO_LINHA nvarchar(510) NULL
'
EXEC sp_executesql @CMD

-- 2) Atualiza ModeloVeiculoWF (VOLKS/MAN) — lote próprio.
SELECT @CMD = '
    UPDATE a
    SET a.ModeloVeiculoWF = m.ModeloVeiculo_Codigo
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
    LEFT JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.ModeloVeiculo m ON (
        RTRIM(LTRIM(m.ModeloVeiculo_ModeloMarca)) = RTRIM(LTRIM(a.CODIGO_LINHA)) COLLATE Latin1_General_CI_AI
        AND RTRIM(LTRIM(a.CODIGO_LINHA)) <> ''''
        AND a.Marca_CodigoWF = m.ModeloVeiculo_MarcaCod
    )
    WHERE a.Marca_CodigoWF IN (36, 54)
'
EXEC sp_executesql @CMD

-- 3) Gera ModeloVeiculo_DePara — lote próprio (colunas já existem).
SELECT @CMD = '
    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ModeloVeiculo_DePara
        (mod_cd, mod_ds, ModeloVeiculo_Codigo, ModeloVeiculo_Descricao, ModeloVeiculo_MarcaCod,
         ModeloVeiculo_ModeloMarca, molicar_cd, ModeloVeiculo_TabelaMolicar, MARCA_CODIGO, CODIGO_LINHA)
    SELECT DISTINCT
        mod_cd = ISNULL(a.MODELO_CODIGO, ''''),
        mod_ds = ISNULL(RTRIM(LTRIM(a.MODELO_DESCRICAO)), ''''),
        ModeloVeiculo_Codigo = ''S/DePara'',
        ModeloVeiculo_Descricao = ''S/DePara'',
        ModeloVeiculo_MarcaCod = ISNULL(CAST(a.Marca_CodigoWF AS varchar), ''S/DePara''),
        ModeloVeiculo_ModeloMarca = ''S/DePara'',
        molicar_cd = '''',
        ModeloVeiculo_TabelaMolicar = '''',
        MARCA_CODIGO = ISNULL(a.VEICULO_MARCA_CODIGO, ''''),
        CODIGO_LINHA = ISNULL(a.CODIGO_LINHA, '''')
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
    WHERE a.Flag = 1
        AND a.ModeloVeiculoWF IS NULL
        AND NOT EXISTS (
            SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ModeloVeiculo_DePara b
            WHERE ISNULL(a.VEICULO_MARCA_CODIGO, '''') = b.MARCA_CODIGO COLLATE database_default
              AND ISNULL(a.MODELO_CODIGO, '''') = b.mod_cd COLLATE database_default
              AND ISNULL(a.CODIGO_LINHA, '''') = b.CODIGO_LINHA COLLATE database_default
        )
'
EXEC sp_executesql @CMD

-- 4) Aplica De/Para por descrição e marca — lote próprio.
SELECT @CMD = '
    UPDATE a
    SET a.ModeloVeiculo_Codigo = b.ModeloVeiculo_Codigo,
        a.ModeloVeiculo_Descricao = b.ModeloVeiculo_Descricao,
        a.ModeloVeiculo_ModeloMarca = b.ModeloVeiculo_ModeloMarca
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ModeloVeiculo_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.ModeloVeiculo b ON (
        RTRIM(LTRIM(b.ModeloVeiculo_Descricao)) = RTRIM(LTRIM(a.mod_ds)) COLLATE Latin1_General_CI_AI
        AND a.ModeloVeiculo_MarcaCod = b.ModeloVeiculo_MarcaCod
    )
    WHERE ISNUMERIC(a.ModeloVeiculo_Codigo) = 0
      AND ISNUMERIC(a.ModeloVeiculo_MarcaCod) = 1
'
EXEC sp_executesql @CMD
GO
-- <<< FIM: up_01_Veiculo_DePara_ModeloVeiculo.sql
GO


-- >>> INICIO: up_02_Veiculo_DePara_CorExterna.sql
-- =============================================================================
-- Layout: Veiculo De/Para — CorExterna
-- Procedure: up_02_Veiculo_DePara_CorExterna (@BancoDadosGX, @BancoWF)
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_02_Veiculo_DePara_CorExterna' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_02_Veiculo_DePara_CorExterna;
GO

CREATE PROCEDURE dbo.up_02_Veiculo_DePara_CorExterna
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX)
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

SELECT @CMD = '
    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.CorExterna_DePara
        (cor_cdext, cor_ds, Cor_Codigo, Cor_Descricao)
    SELECT DISTINCT
        cor_cdext = ISNULL(a.COR_EXTERNA_CODIGO, ''''),
        cor_ds = ISNULL(a.COR_EXTERNA_DESCRICAO, ''''),
        Cor_Codigo = ''S/DePara'',
        Cor_Descricao = ''S/DePara''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
    WHERE a.COR_EXTERNA_CODIGO IS NOT NULL
        AND a.COR_EXTERNA_CODIGO <> ''''
        AND a.Flag = 1
        AND NOT EXISTS (
            SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.CorExterna_DePara b
            WHERE ISNULL(a.COR_EXTERNA_CODIGO, '''') = ISNULL(b.cor_cdext, '''')
        )

    UPDATE a
    SET a.Cor_Codigo = b.Cor_Codigo,
        a.Cor_Descricao = b.Cor_Descricao
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.CorExterna_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Cor b ON b.Cor_Descricao = a.cor_ds COLLATE Latin1_General_CI_AI AND b.Cor_Tipo <> ''I''
    WHERE ISNUMERIC(a.Cor_Codigo) = 0
'
EXEC sp_executesql @CMD
GO
-- <<< FIM: up_02_Veiculo_DePara_CorExterna.sql
GO


-- >>> INICIO: up_03_Veiculo_DePara_CorInterna.sql
-- =============================================================================
-- Layout: Veiculo De/Para — CorInterna
-- Procedure: up_03_Veiculo_DePara_CorInterna (@BancoDadosGX, @BancoWF)
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_03_Veiculo_DePara_CorInterna' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_03_Veiculo_DePara_CorInterna;
GO

CREATE PROCEDURE dbo.up_03_Veiculo_DePara_CorInterna
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX)
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

SELECT @CMD = '
    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.CorInterna_DePara
        (cor_cd, cor_ds, Cor_Codigo, Cor_Descricao)
    SELECT DISTINCT
        cor_cd = ISNULL(a.COR_INTERNA_CODIGO, ''''),
        cor_ds = ISNULL(a.COR_INTERNA_DESCRICAO, ''''),
        Cor_Codigo = ''S/DePara'',
        Cor_Descricao = ''S/DePara''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
    WHERE a.Flag = 1
        AND NOT EXISTS (
            SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.CorInterna_DePara b
            WHERE ISNULL(a.COR_INTERNA_CODIGO, '''') = ISNULL(b.cor_cd, '''')
        )

    UPDATE a
    SET a.Cor_Codigo = b.Cor_Codigo,
        a.Cor_Descricao = b.Cor_Descricao
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.CorInterna_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Cor b ON b.Cor_Descricao = a.cor_ds COLLATE Latin1_General_CI_AI AND b.Cor_Tipo = ''I''
    WHERE ISNUMERIC(a.Cor_Codigo) = 0
'
EXEC sp_executesql @CMD
GO
-- <<< FIM: up_03_Veiculo_DePara_CorInterna.sql
GO


-- >>> INICIO: up_04_Veiculo_DePara_VeiculoAno.sql
-- =============================================================================
-- Layout: Veiculo De/Para — VeiculoAno
-- Procedure: up_04_Veiculo_DePara_VeiculoAno (@BancoDadosGX, @BancoWF)
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_04_Veiculo_DePara_VeiculoAno' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_04_Veiculo_DePara_VeiculoAno;
GO

CREATE PROCEDURE dbo.up_04_Veiculo_DePara_VeiculoAno
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX)
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

SELECT @CMD = '
    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.VeiculoAno_DePara
        (ve_fabmod, VeiculoAno_Codigo, VeiculoAno_Exibicao)
    SELECT DISTINCT
        ve_fabmod = ISNULL(a.Ve_FabMod, ''''),
        VeiculoAno_Codigo = ''S/DePara'',
        VeiculoAno_Exibicao = ''S/DePara''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
    WHERE a.Flag = 1
        AND NOT EXISTS (
            SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.VeiculoAno_DePara b
            WHERE ISNULL(a.Ve_FabMod, '''') = ISNULL(b.ve_fabmod, '''')
        )

    UPDATE a
    SET a.VeiculoAno_Codigo = b.VeiculoAno_Codigo,
        a.VeiculoAno_Exibicao = b.VeiculoAno_Exibicao
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.VeiculoAno_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.VeiculoAno b ON b.VeiculoAno_Exibicao = a.ve_fabmod COLLATE Latin1_General_CI_AI
    WHERE ISNUMERIC(a.VeiculoAno_Codigo) = 0
'
EXEC sp_executesql @CMD
GO
-- <<< FIM: up_04_Veiculo_DePara_VeiculoAno.sql
GO


-- >>> INICIO: up_05_Veiculo_DePara_Estado.sql
-- =============================================================================
-- Layout: Veiculo De/Para — Estado
-- Procedure: up_05_Veiculo_DePara_Estado (@BancoDadosGX, @BancoWF)
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_05_Veiculo_DePara_Estado' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_05_Veiculo_DePara_Estado;
GO

CREATE PROCEDURE dbo.up_05_Veiculo_DePara_Estado
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX)
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

SELECT @CMD = '
    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Estado_DePara
        (uf_cd, uf_nm, Estado_Codigo, Estado_Nome, Tabela)
    SELECT DISTINCT
        uf_cd = ISNULL(RTRIM(LTRIM(a.ESTADO_PLACA)), ''''),
        uf_nm = '''',
        Estado_Codigo = ''S/DePara'',
        Estado_Nome = ''S/DePara'',
        Tabela = ''Veiculo''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
    WHERE a.Flag = 1
        AND NOT EXISTS (
            SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Estado_DePara b
            WHERE b.UF_CD = ISNULL(RTRIM(LTRIM(a.ESTADO_PLACA)), '''') COLLATE Latin1_General_CI_AI
        )

    UPDATE a
    SET a.Estado_Codigo = b.Estado_Codigo,
        a.Estado_Nome = b.Estado_Nome
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Estado_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Estado b ON b.Estado_Codigo = a.uf_cd
    WHERE ISNUMERIC(a.Estado_Codigo) = 0

    UPDATE a
    SET a.Pais_Codigo = b.Pais_Codigo
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Estado_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Estado b ON a.Estado_Codigo = b.Estado_Codigo COLLATE database_default
    WHERE a.Pais_Codigo IS NULL
'
EXEC sp_executesql @CMD
GO
-- <<< FIM: up_05_Veiculo_DePara_Estado.sql
GO


-- >>> INICIO: up_06_Veiculo_DePara_Municipio.sql
-- =============================================================================
-- Layout: Veiculo De/Para — Municipio
-- Procedure: up_06_Veiculo_DePara_Municipio (@BancoDadosGX, @BancoWF)
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_06_Veiculo_DePara_Municipio' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_06_Veiculo_DePara_Municipio;
GO

CREATE PROCEDURE dbo.up_06_Veiculo_DePara_Municipio
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX)
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

SELECT @CMD = '
    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Municipio_DePara
        (cg_cidade, Municipio_IBGE, uf_cd, Municipio_Codigo, Municipio_Nome, Estado_Codigo, Tabela)
    SELECT DISTINCT
        cg_cidade = ISNULL(RTRIM(LTRIM(UPPER(a.MUNICIPIO_PLACA))) COLLATE SQL_Latin1_General_CP1253_CI_AI, ''''),
        Municipio_IBGE = '''',
        uf_cd = ISNULL(RTRIM(LTRIM(a.ESTADO_PLACA)), ''''),
        Municipio_Codigo = ''S/DePara'',
        Municipio_Nome = ''S/DePara'',
        Estado_Codigo = ''S/DePara'',
        Tabela = ''Veiculo''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
    WHERE a.Flag = 1
        AND NOT EXISTS (
            SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Municipio_DePara b
            WHERE RTRIM(LTRIM(ISNULL(a.MUNICIPIO_PLACA, ''''))) = RTRIM(LTRIM(ISNULL(b.cg_cidade, ''''))) COLLATE SQL_Latin1_General_CP1253_CI_AI
              AND RTRIM(LTRIM(ISNULL(a.ESTADO_PLACA, ''''))) = ISNULL(b.uf_cd, '''') COLLATE SQL_Latin1_General_CP1253_CI_AI
        )

    UPDATE a
    SET a.Municipio_Codigo = b.Municipio_Codigo,
        a.Municipio_Nome = b.Municipio_Nome,
        a.Estado_Codigo = b.Estado_Codigo
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Municipio_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Municipio b ON (
        RTRIM(LTRIM(a.cg_cidade)) = RTRIM(LTRIM(b.Municipio_Nome)) COLLATE SQL_Latin1_General_CP1253_CI_AI
        AND RTRIM(LTRIM(a.uf_cd)) = b.Estado_Codigo COLLATE SQL_Latin1_General_CP1253_CI_AI
    )
    WHERE ISNUMERIC(a.Municipio_Codigo) = 0
'
EXEC sp_executesql @CMD
GO
-- <<< FIM: up_06_Veiculo_DePara_Municipio.sql
GO


-- >>> INICIO: up_07_Veiculo_DePara_Marca.sql
-- =============================================================================
-- Layout: Veiculo De/Para — Marca
-- Procedure: up_07_Veiculo_DePara_Marca (@BancoDadosGX, @BancoWF)
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_07_Veiculo_DePara_Marca' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_07_Veiculo_DePara_Marca;
GO

CREATE PROCEDURE dbo.up_07_Veiculo_DePara_Marca
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX)
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

SELECT @CMD = '
    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Marca_DePara
        (marc_cd, marc_ds, Marca_Codigo, Marca_Descricao, Marca_Sigla)
    SELECT DISTINCT
        marc_cd = ISNULL(a.VEICULO_MARCA_CODIGO, ''''),
        marc_ds = ISNULL(a.VEICULO_MARCA_DESCRICAO, ''''),
        Marca_Codigo = ''S/DePara'',
        Marca_Descricao = ''S/DePara'',
        Marca_Sigla = ''S/DePara''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
    WHERE a.Flag = 1
        AND NOT EXISTS (
            SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Marca_DePara b
            WHERE b.marc_cd = ISNULL(a.VEICULO_MARCA_CODIGO, '''') COLLATE Latin1_General_CI_AI
        )

    UPDATE a
    SET a.Marca_Codigo = b.Marca_Codigo,
        a.Marca_Descricao = b.Marca_Descricao,
        a.Marca_Sigla = b.Marca_Sigla
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Marca_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Marca b ON b.Marca_Descricao = a.marc_ds COLLATE Latin1_General_CI_AI

    UPDATE a
    SET a.ModeloVeiculo_MarcaCod = b.Marca_Codigo
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ModeloVeiculo_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Marca_DePara b ON b.marc_cd = a.MARCA_CODIGO COLLATE database_default
    WHERE ISNUMERIC(a.ModeloVeiculo_MarcaCod) = 0
'
EXEC sp_executesql @CMD
GO
-- <<< FIM: up_07_Veiculo_DePara_Marca.sql
GO


-- >>> INICIO: up_01_Financeiro_DePara_AgenteCobrador.sql
-- =============================================================================
-- Layout: Financeiro De/Para
-- Procedure: up_01_Financeiro_DePara_AgenteCobrador (@BancoDadosGX, @BancoWF)
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Financeiro_DePara_AgenteCobrador' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_01_Financeiro_DePara_AgenteCobrador;
GO

CREATE PROCEDURE dbo.up_01_Financeiro_DePara_AgenteCobrador
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX)
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

SELECT @CMD = '
    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.AgenteCobrador_DePara
        (agc_cd, agc_nm, AgenteCobrador_Codigo, AgenteCobrador_Descricao, Origem)
    SELECT DISTINCT
        agc_cd = ISNULL(a.AGENTECOBRADOR_CODIGO, ''''),
        agc_nm = ISNULL(a.AGENTECOBRADOR_DESCRICAO, ''''),
        AgenteCobrador_Codigo = ''S/DePara'',
        AgenteCobrador_Descricao = ''S/DePara'',
        Origem = (CASE
                    WHEN (a.TIPO_MOVFINANCEIRO = ''P'') THEN ''Obrigações''
                    WHEN (a.TIPO_MOVFINANCEIRO = ''R'') THEN ''Títulos''
                    ELSE ''''
                END)
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Titulo_MG a
    WHERE
        a.Flag = 1 AND
        NOT EXISTS (
            SELECT 1
            FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.AgenteCobrador_DePara b
            WHERE ISNULL(a.AGENTECOBRADOR_CODIGO, '''') = ISNULL(b.agc_cd, '''') COLLATE DATABASE_DEFAULT
        )

    UPDATE a
    SET
        a.AgenteCobrador_Codigo = b.AgenteCobrador_Codigo,
        a.AgenteCobrador_Descricao = b.AgenteCobrador_Descricao
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.AgenteCobrador_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.AgenteCobrador b
        ON b.AgenteCobrador_Descricao = a.agc_nm COLLATE Latin1_General_CI_AI
    WHERE a.AgenteCobrador_Codigo = ''S/DePara''
'
EXEC sp_executesql @CMD
GO
-- <<< FIM: up_01_Financeiro_DePara_AgenteCobrador.sql
GO


-- >>> INICIO: up_02_Financeiro_DePara_ContaGerencial.sql
-- =============================================================================
-- Layout: Financeiro De/Para
-- Procedure: up_02_Financeiro_DePara_ContaGerencial (@BancoDadosGX, @BancoWF)
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_02_Financeiro_DePara_ContaGerencial' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_02_Financeiro_DePara_ContaGerencial;
GO

CREATE PROCEDURE dbo.up_02_Financeiro_DePara_ContaGerencial
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)
DECLARE @BancoGX SYSNAME = LTRIM(RTRIM(@BancoDadosGX))
DECLARE @BancoWFs SYSNAME = LTRIM(RTRIM(@BancoWF))

IF NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoGX)
BEGIN
    PRINT 'O < ' + @BancoGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

-- 1) Garante colunas extras em ContaGerencial_DePara (batch separado — SQL Server
--    não permite ADD + referência no mesmo batch).
SELECT @CMD = N'
    IF OBJECT_ID(N''' + QUOTENAME(@BancoGX) + N'.dbo.ContaGerencial_DePara'', N''U'') IS NULL
    BEGIN
        RAISERROR(''Tabela ContaGerencial_DePara não existe em %s'', 16, 1, ''' + @BancoGX + N''')
        RETURN
    END

    IF NOT EXISTS (
        SELECT 1 FROM ' + QUOTENAME(@BancoGX) + N'.sys.columns col
        INNER JOIN ' + QUOTENAME(@BancoGX) + N'.sys.objects obj ON col.object_id = obj.object_id
        WHERE col.name = ''ContaGerencial_Tipo'' AND obj.name = ''ContaGerencial_DePara''
    )
        ALTER TABLE ' + QUOTENAME(@BancoGX) + N'.dbo.ContaGerencial_DePara ADD ContaGerencial_Tipo char(1) NULL

    IF NOT EXISTS (
        SELECT 1 FROM ' + QUOTENAME(@BancoGX) + N'.sys.columns col
        INNER JOIN ' + QUOTENAME(@BancoGX) + N'.sys.objects obj ON col.object_id = obj.object_id
        WHERE col.name = ''ContaGerencial_Nivel'' AND obj.name = ''ContaGerencial_DePara''
    )
        ALTER TABLE ' + QUOTENAME(@BancoGX) + N'.dbo.ContaGerencial_DePara ADD ContaGerencial_Nivel char(1) NULL

    -- Colunas de origem em Titulo_MG (layout pode omitir descrição)
    IF NOT EXISTS (
        SELECT 1 FROM ' + QUOTENAME(@BancoGX) + N'.sys.columns col
        INNER JOIN ' + QUOTENAME(@BancoGX) + N'.sys.objects obj ON col.object_id = obj.object_id
        WHERE col.name = ''CONTAGERENCIAL_CODIGO'' AND obj.name = ''Titulo_MG''
    )
        ALTER TABLE ' + QUOTENAME(@BancoGX) + N'.dbo.Titulo_MG ADD CONTAGERENCIAL_CODIGO VARCHAR(MAX) NULL

    IF NOT EXISTS (
        SELECT 1 FROM ' + QUOTENAME(@BancoGX) + N'.sys.columns col
        INNER JOIN ' + QUOTENAME(@BancoGX) + N'.sys.objects obj ON col.object_id = obj.object_id
        WHERE col.name = ''CONTAGERENCIAL_DESCRICAO'' AND obj.name = ''Titulo_MG''
    )
        ALTER TABLE ' + QUOTENAME(@BancoGX) + N'.dbo.Titulo_MG ADD CONTAGERENCIAL_DESCRICAO VARCHAR(MAX) NULL
'
EXEC sp_executesql @CMD

-- 2) Carga + match WF (após as colunas existirem)
SELECT @CMD = N'
    INSERT INTO ' + QUOTENAME(@BancoGX) + N'.dbo.ContaGerencial_DePara
        (pcg_cd, pcg_ds, ContaGerencial_Codigo, ContaGerencial_Identificador, ContaGerencial_Descricao, Origem)
    SELECT DISTINCT
        pcg_cd = ISNULL(a.CONTAGERENCIAL_CODIGO, ''''),
        pcg_ds = ISNULL(a.CONTAGERENCIAL_DESCRICAO, ''''),
        ContaGerencial_Codigo = ''S/DePara'',
        ContaGerencial_Identificador = ''S/DePara'',
        ContaGerencial_Descricao = ''S/DePara'',
        Origem = (CASE
                    WHEN (a.TIPO_MOVFINANCEIRO = ''P'') THEN ''Obrigações''
                    WHEN (a.TIPO_MOVFINANCEIRO = ''R'') THEN ''Títulos''
                    ELSE ''''
                END)
    FROM ' + QUOTENAME(@BancoGX) + N'.dbo.Titulo_MG a
    WHERE
        a.Flag = 1 AND
        NOT EXISTS (
            SELECT 1
            FROM ' + QUOTENAME(@BancoGX) + N'.dbo.ContaGerencial_DePara b
            WHERE ISNULL(a.CONTAGERENCIAL_CODIGO, '''') = ISNULL(b.pcg_cd, '''') COLLATE DATABASE_DEFAULT
        )

    UPDATE a
    SET
        a.ContaGerencial_Codigo = b.ContaGerencial_Codigo,
        a.ContaGerencial_Descricao = b.ContaGerencial_Descricao,
        a.ContaGerencial_Tipo = b.ContaGerencial_Tipo,
        a.ContaGerencial_Nivel = b.ContaGerencial_Nivel
    FROM ' + QUOTENAME(@BancoGX) + N'.dbo.ContaGerencial_DePara a
    INNER JOIN ' + QUOTENAME(@BancoWFs) + N'.dbo.ContaGerencial b
        ON b.ContaGerencial_Descricao = a.pcg_ds COLLATE Latin1_General_CI_AI
    WHERE a.ContaGerencial_Codigo = ''S/DePara''
'
EXEC sp_executesql @CMD
GO
-- <<< FIM: up_02_Financeiro_DePara_ContaGerencial.sql
GO


-- >>> INICIO: up_03_Financeiro_DePara_TipoTitulo.sql
-- =============================================================================
-- Layout: Financeiro De/Para
-- Procedure: up_03_Financeiro_DePara_TipoTitulo (@BancoDadosGX, @BancoWF)
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_03_Financeiro_DePara_TipoTitulo' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_03_Financeiro_DePara_TipoTitulo;
GO

CREATE PROCEDURE dbo.up_03_Financeiro_DePara_TipoTitulo
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX)
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

SELECT @CMD = '
    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoTitulo_DePara
        (tpt_cd, tpt_ds, tpo_cd, tpo_ds, TipoTitulo_Codigo, TipoTitulo_Descricao,
         TipoTitulo_PermissaoUso, TipoTituloEmp_PessoaCod, Origem)
    SELECT DISTINCT
        tpt_cd = '''',
        tpt_ds = '''',
        tpo_cd = ISNULL(a.TIPOTITULO_CODIGO, ''''),
        tpo_ds = ISNULL(a.TIPOTITULO_DESCRICAO, ''''),
        TipoTitulo_Codigo = ''S/DePara'',
        TipoTitulo_Descricao = ''S/DePara'',
        TipoTitulo_PermissaoUso = ''P'',
        TipoTituloEmp_PessoaCod = '''',
        Origem = ''Obrigações''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Titulo_MG a
    WHERE
        a.Flag = 1 AND
        a.TIPO_MOVFINANCEIRO = ''P'' AND
        NOT EXISTS(
            SELECT 1
            FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoTitulo_DePara b
            WHERE b.tpo_cd = a.TIPOTITULO_CODIGO collate database_default AND a.TIPO_MOVFINANCEIRO = ''P''
        )

    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoTitulo_DePara
        (tpt_cd, tpt_ds, tpo_cd, tpo_ds, TipoTitulo_Codigo, TipoTitulo_Descricao,
         TipoTitulo_PermissaoUso, TipoTituloEmp_PessoaCod, Origem)
    SELECT DISTINCT
        tpt_cd = ISNULL(a.TIPOTITULO_CODIGO, ''''),
        tpt_ds = ISNULL(a.TIPOTITULO_DESCRICAO, ''''),
        tpo_cd = '''',
        tpo_ds = '''',
        TipoTitulo_Codigo = ''S/DePara'',
        TipoTitulo_Descricao = ''S/DePara'',
        TipoTitulo_PermissaoUso = ''R'',
        TipoTituloEmp_PessoaCod = '''',
        Origem = ''Títulos''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Titulo_MG a
    WHERE
        a.Flag = 1 AND
        a.TIPO_MOVFINANCEIRO = ''R'' AND
        NOT EXISTS(
            SELECT 1
            FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoTitulo_DePara b
            WHERE b.tpt_cd = a.TIPOTITULO_CODIGO collate database_default AND a.TIPO_MOVFINANCEIRO = ''R''
        )

    UPDATE a
    SET
        a.TipoTitulo_Codigo = b.TipoTitulo_Codigo,
        a.TipoTitulo_Descricao = b.TipoTitulo_Descricao,
        a.TipoTitulo_PermissaoUso = b.TipoTitulo_PermissaoUso
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoTitulo_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.TipoTitulo b ON
        b.TipoTitulo_Descricao = RTRIM(LTRIM(a.tpt_ds)) COLLATE Latin1_General_CI_AI AND
        b.TipoTitulo_PermissaoUso = a.TipoTitulo_PermissaoUso
    WHERE a.TipoTitulo_Codigo = ''S/DePara''
'
EXEC sp_executesql @CMD
GO
-- <<< FIM: up_03_Financeiro_DePara_TipoTitulo.sql
GO


-- >>> INICIO: up_04_Financeiro_DePara_Departamento.sql
-- =============================================================================
-- Layout: Financeiro De/Para
-- Procedure: up_04_Financeiro_DePara_Departamento (@BancoDadosGX, @BancoWF)
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_04_Financeiro_DePara_Departamento' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_04_Financeiro_DePara_Departamento;
GO

CREATE PROCEDURE dbo.up_04_Financeiro_DePara_Departamento
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX)
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

SELECT @CMD = '
    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Departamento_Depara
        (dep_cd, dep_nm, Departamento_Codigo, Departamento_Descricao, Departamento_Sigla)
    SELECT DISTINCT
        dep_cd = ISNULL(a.DEPARTAMENTO_CODIGO, ''''),
        dep_nm = ISNULL(a.DEPARTAMENTO_DESCRICAO, ''''),
        Departamento_Codigo = ''S/DePara'',
        Departamento_Descricao = ''S/DePara'',
        Departamento_Sigla = ''S/DePara''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Titulo_MG a
    WHERE
        NOT EXISTS (
            SELECT 1
            FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Departamento_Depara b
            WHERE b.dep_cd = ISNULL(a.DEPARTAMENTO_CODIGO, '''') collate database_default
        )

    UPDATE a
    SET
        a.Departamento_Codigo = b.Departamento_Codigo,
        a.Departamento_Descricao = b.Departamento_Descricao,
        a.Departamento_Sigla = b.Departamento_Sigla
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Departamento_Depara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Departamento b
        ON b.Departamento_Descricao = a.dep_nm COLLATE Latin1_General_CI_AI
    WHERE a.Departamento_Codigo = ''S/DePara''
'
EXEC sp_executesql @CMD
GO
-- <<< FIM: up_04_Financeiro_DePara_Departamento.sql
GO


-- >>> INICIO: up_05_Financeiro_DePara_NaturezaOperacao.sql
-- =============================================================================
-- Layout: Financeiro De/Para
-- Procedure: up_05_Financeiro_DePara_NaturezaOperacao (@BancoDadosGX, @BancoWF)
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_05_Financeiro_DePara_NaturezaOperacao' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_05_Financeiro_DePara_NaturezaOperacao;
GO

CREATE PROCEDURE dbo.up_05_Financeiro_DePara_NaturezaOperacao
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX)
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

SELECT @CMD = '
    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.NaturezaOperacao_DePara
        (me_cd, me_ds, dep_cd, Tipo, NaturezaOperacao_Codigo, NaturezaOperacao_Descricao,
         Departamento_Codigo, Procedure_Origem)
    SELECT DISTINCT
        me_cd = ISNULL(a.NATUREZAOPERACAO_CODIGO, ''''),
        me_ds = ISNULL(a.NATUREZAOPERACAO_DESCRICAO, ''''),
        dep_cd = ISNULL(a.DEPARTAMENTO_CODIGO, ''''),
        Tipo = '''',
        NaturezaOperacao_Codigo = ''S/DePara'',
        NaturezaOperacao_Descricao = ''S/DePara'',
        Departamento_Codigo = ''S/DePara'',
        Procedure_Origem = (CASE
                                WHEN (a.TIPO_MOVFINANCEIRO = ''P'') THEN ''Obrigações''
                                WHEN (a.TIPO_MOVFINANCEIRO = ''R'') THEN ''Títulos''
                                ELSE ''''
                            END)
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Titulo_MG a
    WHERE
        a.Flag = 1 AND
        NOT EXISTS (
            SELECT 1
            FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.NaturezaOperacao_DePara d
            WHERE ISNULL(d.me_cd, '''') = ISNULL(a.NATUREZAOPERACAO_CODIGO, '''') COLLATE DATABASE_DEFAULT
        )

    UPDATE a
    SET
        a.NaturezaOperacao_Codigo = b.NaturezaOperacao_Codigo,
        a.NaturezaOperacao_Descricao = b.NaturezaOperacao_Descricao
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.NaturezaOperacao_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.NaturezaOperacao b
        ON b.NaturezaOperacao_Descricao = a.me_ds COLLATE DATABASE_DEFAULT
    WHERE a.NaturezaOperacao_Codigo = ''S/DePara''

    UPDATE a
    SET a.Departamento_Codigo = b.Departamento_Codigo
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.NaturezaOperacao_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Departamento_DePara b ON b.dep_cd = a.dep_cd
    WHERE a.Departamento_Codigo = ''S/DePara''
'
EXEC sp_executesql @CMD
GO
-- <<< FIM: up_05_Financeiro_DePara_NaturezaOperacao.sql
GO


-- >>> INICIO: up_06_Financeiro_DePara_Banco.sql
-- =============================================================================
-- Layout: Financeiro De/Para
-- Procedure: up_06_Financeiro_DePara_Banco (@BancoDadosGX, @BancoWF)
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_06_Financeiro_DePara_Banco' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_06_Financeiro_DePara_Banco;
GO

CREATE PROCEDURE dbo.up_06_Financeiro_DePara_Banco
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX)
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

SELECT @CMD = '
    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Banco_DePara
        (ban_cd, ban_ds, Banco_Codigo, Banco_Descricao, Banco_Sigla)
    SELECT DISTINCT
        ban_cd = ISNULL(a.CODIGO_BANCO, ''''),
        ban_ds = '''',
        Banco_Codigo = ''S/DePara'',
        Banco_Descricao = ''S/DePara'',
        Banco_Sigla = ''S/DePara''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Titulo_MG a
    WHERE
        a.Flag = 1 AND
        NOT EXISTS (
            SELECT 1
            FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Banco_DePara b
            WHERE ISNULL(a.CODIGO_BANCO, '''') = ISNULL(b.ban_cd, '''') COLLATE DATABASE_DEFAULT
        )

    UPDATE a
    SET
        a.Banco_Codigo = b.Banco_Codigo,
        a.Banco_Descricao = b.Banco_Descricao,
        a.Banco_Sigla = b.Banco_Sigla
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Banco_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Banco b
        ON b.Banco_Sigla = a.ban_cd COLLATE Latin1_General_CI_AI
    WHERE a.Banco_Codigo = ''S/DePara''
'
EXEC sp_executesql @CMD
GO
-- <<< FIM: up_06_Financeiro_DePara_Banco.sql
GO


-- >>> INICIO: up_01_Adiantamento_DePara_TipoFichaRazao.sql
-- =============================================================================
-- Layout: Adiantamento De/Para
-- Procedure: up_01_Adiantamento_DePara_TipoFichaRazao (@BancoDadosGX, @BancoWF)
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Adiantamento_DePara_TipoFichaRazao' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_01_Adiantamento_DePara_TipoFichaRazao;
GO

CREATE PROCEDURE dbo.up_01_Adiantamento_DePara_TipoFichaRazao
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX)
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

SELECT @CMD = '
    IF OBJECT_ID(''' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoFichaRazao_DePara'', ''U'') IS NULL
    BEGIN
        RAISERROR(''Tabela TipoFichaRazao_DePara não existe.'', 16, 1)
        RETURN
    END
'
EXEC sp_executesql @CMD

-- Receber (R) → FRT; Pagar (P) → FRO (mesmo padrão de TipoTitulo)
SELECT @CMD = '
    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoFichaRazao_DePara
        (frt_cd, frt_ds, fro_cd, fro_ds, dep_cd, dep_nm,
         TipoFichaRazao_Codigo, TipoFichaRazao_Descricao, TipoFichaRazao_Natureza,
         Departamento_Codigo, Origem)
    SELECT DISTINCT
        frt_cd = ISNULL(a.TIPO_FICHARAZAO, ''''),
        frt_ds = ISNULL(a.DESCRICAO_FICHARAZAO, ''''),
        fro_cd = '''',
        fro_ds = '''',
        dep_cd = '''',
        dep_nm = '''',
        TipoFichaRazao_Codigo = ''S/DePara'',
        TipoFichaRazao_Descricao = ''S/DePara'',
        TipoFichaRazao_Natureza = a.TIPO_MOVFINANCEIRO,
        Departamento_Codigo = '''',
        Origem = ''Adiantamentos''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.FichaRazao_MG a
    WHERE
        a.Flag = 1 AND
        a.TIPO_MOVFINANCEIRO = ''R'' AND
        NOT EXISTS (
            SELECT 1
            FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoFichaRazao_DePara b
            WHERE ISNULL(b.frt_cd, '''') = ISNULL(a.TIPO_FICHARAZAO, '''') COLLATE DATABASE_DEFAULT
              AND ISNULL(b.frt_ds, '''') = ISNULL(a.DESCRICAO_FICHARAZAO, '''') COLLATE DATABASE_DEFAULT
        )

    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoFichaRazao_DePara
        (frt_cd, frt_ds, fro_cd, fro_ds, dep_cd, dep_nm,
         TipoFichaRazao_Codigo, TipoFichaRazao_Descricao, TipoFichaRazao_Natureza,
         Departamento_Codigo, Origem)
    SELECT DISTINCT
        frt_cd = '''',
        frt_ds = '''',
        fro_cd = ISNULL(a.TIPO_FICHARAZAO, ''''),
        fro_ds = ISNULL(a.DESCRICAO_FICHARAZAO, ''''),
        dep_cd = '''',
        dep_nm = '''',
        TipoFichaRazao_Codigo = ''S/DePara'',
        TipoFichaRazao_Descricao = ''S/DePara'',
        TipoFichaRazao_Natureza = a.TIPO_MOVFINANCEIRO,
        Departamento_Codigo = '''',
        Origem = ''Adiantamentos''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.FichaRazao_MG a
    WHERE
        a.Flag = 1 AND
        a.TIPO_MOVFINANCEIRO = ''P'' AND
        NOT EXISTS (
            SELECT 1
            FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoFichaRazao_DePara b
            WHERE ISNULL(b.fro_cd, '''') = ISNULL(a.TIPO_FICHARAZAO, '''') COLLATE DATABASE_DEFAULT
              AND ISNULL(b.fro_ds, '''') = ISNULL(a.DESCRICAO_FICHARAZAO, '''') COLLATE DATABASE_DEFAULT
        )

    UPDATE a
    SET
        a.TipoFichaRazao_Codigo = b.TipoFichaRazao_Codigo,
        a.TipoFichaRazao_Descricao = b.TipoFichaRazao_Descricao,
        a.TipoFichaRazao_Natureza = b.TipoFichaRazao_Natureza
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoFichaRazao_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.TipoFichaRazao b
        ON b.TipoFichaRazao_Descricao = RTRIM(LTRIM(
            CASE WHEN ISNULL(a.frt_ds, '''') <> '''' THEN a.frt_ds ELSE a.fro_ds END
        )) COLLATE Latin1_General_CI_AI
    WHERE a.TipoFichaRazao_Codigo = ''S/DePara''
'
EXEC sp_executesql @CMD
GO
-- <<< FIM: up_01_Adiantamento_DePara_TipoFichaRazao.sql
GO


-- >>> INICIO: up_01_MovimentoEstoque_DePara_NaturezaOperacao.sql
-- =============================================================================
-- Layout: MovimentoEstoque De/Para
-- Tabela : NaturezaOperacao_DePara
-- Procedure: up_01_MovimentoEstoque_DePara_NaturezaOperacao (@BancoDadosGX, @BancoWF)
-- Autor: Aroldo Santana
-- Data de alteração: 07/07/2026
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_MovimentoEstoque_DePara_NaturezaOperacao' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_01_MovimentoEstoque_DePara_NaturezaOperacao;
GO

CREATE PROCEDURE dbo.up_01_MovimentoEstoque_DePara_NaturezaOperacao
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF (NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX))
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

IF (NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoWF))
BEGIN
    PRINT 'O < ' + @BancoWF + ' > INFORMADO COMO @BancoWF NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

SELECT @CMD = '
PRINT ''==========================================================================================''
PRINT '' 01 - GERA NaturezaOperacao_DePara (origem MovimentoEstoque_MG)''
PRINT ''==========================================================================================''

    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.NaturezaOperacao_DePara
        (me_cd, me_ds, dep_cd, Tipo, NaturezaOperacao_Codigo, NaturezaOperacao_Descricao,
         Departamento_Codigo, Procedure_Origem)
    SELECT DISTINCT
        me_cd = ISNULL(a.MOVIMENTO_CODIGO, ''''),
        me_ds = ISNULL(a.MOVIMENTO_DESCRICAO, ''''),
        dep_cd = ISNULL(a.DEPARTAMENTO_CODIGO, ''''),
        Tipo = ''Historico'',
        NaturezaOperacao_Codigo = ''S/DePara'',
        NaturezaOperacao_Descricao = ''S/DePara'',
        Departamento_Codigo = ''S/DePara'',
        Procedure_Origem = ''MovimentoEstoque''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.MovimentoEstoque_MG a
    WHERE
        NOT EXISTS (
            SELECT 1
            FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.NaturezaOperacao_DePara b
            WHERE b.me_cd = a.MOVIMENTO_CODIGO COLLATE database_default
        )

PRINT ''==========================================================================================''
PRINT '' 02 - Aplica POR DESCRIÇÃO - NaturezaOperacao_DePara''
PRINT ''==========================================================================================''

    UPDATE a
    SET
        a.NaturezaOperacao_Codigo = b.NaturezaOperacao_Codigo,
        a.NaturezaOperacao_Descricao = b.NaturezaOperacao_Descricao
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.NaturezaOperacao_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.NaturezaOperacao b
        ON b.NaturezaOperacao_Descricao = a.me_ds COLLATE Latin1_General_CI_AI
    WHERE a.NaturezaOperacao_Codigo = ''S/DePara''
'
EXEC sp_executesql @CMD
GO
-- <<< FIM: up_01_MovimentoEstoque_DePara_NaturezaOperacao.sql
GO


-- >>> INICIO: up_02_MovimentoEstoque_DePara_Estoque.sql
-- =============================================================================
-- Layout: MovimentoEstoque De/Para
-- Tabela : Estoque_DePara
-- Procedure: up_02_MovimentoEstoque_DePara_Estoque (@BancoDadosGX, @BancoWF)
-- Autor: Aroldo Santana
-- Data de alteração: 07/07/2026
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_02_MovimentoEstoque_DePara_Estoque' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_02_MovimentoEstoque_DePara_Estoque;
GO

CREATE PROCEDURE dbo.up_02_MovimentoEstoque_DePara_Estoque
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF (NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX))
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

IF (NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoWF))
BEGIN
    PRINT 'O < ' + @BancoWF + ' > INFORMADO COMO @BancoWF NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

SELECT @CMD = '
PRINT ''==========================================================================================''
PRINT '' 01 - GERA Estoque_DePara (origem MovimentoEstoque_MG)''
PRINT ''==========================================================================================''

    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Estoque_DePara
        (est_cd, est_ds, Estoque_Codigo, Estoque_Descricao, Estoque_Sigla)
    SELECT DISTINCT
        est_cd = ISNULL(a.ESTOQUE_CODIGO, ''''),
        est_ds = '''',
        Estoque_Codigo = ''S/DePara'',
        Estoque_Descricao = ''S/DePara'',
        Estoque_Sigla = ''S/DePara''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.MovimentoEstoque_MG a
    WHERE
        NOT EXISTS (
            SELECT 1
            FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Estoque_DePara b
            WHERE b.est_cd = ISNULL(a.ESTOQUE_CODIGO, '''') COLLATE database_default
        )

PRINT ''==========================================================================================''
PRINT '' 02 - Aplica POR SIGLA - Estoque_DePara''
PRINT ''==========================================================================================''

    UPDATE a
    SET
        a.Estoque_Codigo = b.Estoque_Codigo,
        a.Estoque_Descricao = b.Estoque_Descricao,
        a.Estoque_Sigla = b.Estoque_Sigla,
        a.Origem = ''MovimentoEstoque''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Estoque_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Estoque b
        ON b.Estoque_Sigla = a.est_cd COLLATE Latin1_General_CI_AI
    WHERE a.Estoque_Codigo = ''S/DePara''
'
EXEC sp_executesql @CMD
GO
-- <<< FIM: up_02_MovimentoEstoque_DePara_Estoque.sql
GO


-- >>> INICIO: up_03_MovimentoEstoque_DePara_Departamento.sql
-- =============================================================================
-- Layout: MovimentoEstoque De/Para
-- Tabela : Departamento_Depara
-- Procedure: up_03_MovimentoEstoque_DePara_Departamento (@BancoDadosGX, @BancoWF)
-- Autor: Aroldo Santana
-- Data de alteração: 07/07/2026
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_03_MovimentoEstoque_DePara_Departamento' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_03_MovimentoEstoque_DePara_Departamento;
GO

CREATE PROCEDURE dbo.up_03_MovimentoEstoque_DePara_Departamento
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF (NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX))
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

IF (NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoWF))
BEGIN
    PRINT 'O < ' + @BancoWF + ' > INFORMADO COMO @BancoWF NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

SELECT @CMD = '
PRINT ''==========================================================================================''
PRINT '' 01 - GERA Departamento_Depara (origem MovimentoEstoque_MG)''
PRINT ''==========================================================================================''

    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Departamento_Depara
        (dep_cd, dep_nm, Departamento_Codigo, Departamento_Descricao, Departamento_Sigla)
    SELECT DISTINCT
        dep_cd = ISNULL(a.DEPARTAMENTO_CODIGO, ''''),
        dep_nm = ISNULL(a.DEPARTAMENTO_DESCRICAO, ''''),
        Departamento_Codigo = ''S/DePara'',
        Departamento_Descricao = ''S/DePara'',
        Departamento_Sigla = ''S/DePara''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.MovimentoEstoque_MG a
    WHERE
        NOT EXISTS (
            SELECT 1
            FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Departamento_Depara b
            WHERE b.dep_cd = ISNULL(a.DEPARTAMENTO_CODIGO, '''') COLLATE database_default
        )

PRINT ''==========================================================================================''
PRINT '' 02 - Aplica POR DESCRIÇÃO - Departamento_Depara''
PRINT ''==========================================================================================''

    UPDATE a
    SET
        a.Departamento_Codigo = b.Departamento_Codigo,
        a.Departamento_Descricao = b.Departamento_Descricao,
        a.Departamento_Sigla = b.Departamento_Sigla
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Departamento_Depara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Departamento b
        ON b.Departamento_Descricao = a.dep_nm COLLATE Latin1_General_CI_AI
    WHERE a.Departamento_Codigo = ''S/DePara''
'
EXEC sp_executesql @CMD
GO
-- <<< FIM: up_03_MovimentoEstoque_DePara_Departamento.sql
GO


-- >>> INICIO: up_01_Fseg_DePara_TipoOS.sql
-- =============================================================================
-- Layout: Fseg_Cab (+ Prd/Srv quando existirem) De/Para TipoOS
-- Procedure: up_01_Fseg_DePara_TipoOS (@BancoDadosGX, @BancoWF)
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Fseg_DePara_TipoOS' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_01_Fseg_DePara_TipoOS;
GO

CREATE PROCEDURE dbo.up_01_Fseg_DePara_TipoOS
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX)
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

-- Garante colunas usadas no insert (lote próprio)
SELECT @CMD = '
    IF NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns WHERE name = ''Origem'' AND object_id = OBJECT_ID(''' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoOS_DePara''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoOS_DePara ADD Origem varchar(50) NULL
'
EXEC sp_executesql @CMD

SELECT @CMD = '
    IF EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects WHERE type = ''U'' AND name = ''Ficha_Cab_MG'')
    BEGIN
        INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoOS_DePara
            (tpos_cd, tpos_ds, tpos_ativa, TipoOS_Codigo, TipoOS_Descricao, TipoOS_Sigla, Origem)
        SELECT DISTINCT
            tpos_cd = ISNULL(a.TIPO_OS_CODIGO, ''''),
            tpos_ds = ISNULL(a.TIPO_OS_DESCRICAO, ''''),
            tpos_ativa = '''',
            TipoOS_Codigo = ''S/DePara'',
            TipoOS_Descricao = ''S/DePara'',
            TipoOS_Sigla = ''S/DePara'',
            Origem = ''Ficha_Seguimento''
        FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG a
        WHERE NOT EXISTS (
            SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoOS_DePara b
            WHERE b.tpos_cd = a.TIPO_OS_CODIGO COLLATE DATABASE_DEFAULT
              AND b.tpos_ds = a.TIPO_OS_DESCRICAO COLLATE DATABASE_DEFAULT
        )
    END
'
EXEC sp_executesql @CMD

SELECT @CMD = '
    IF EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects WHERE type = ''U'' AND name = ''Ficha_Prd_MG'')
    BEGIN
        INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoOS_DePara
            (tpos_cd, tpos_ds, tpos_ativa, TipoOS_Codigo, TipoOS_Descricao, TipoOS_Sigla, Origem)
        SELECT DISTINCT
            tpos_cd = ISNULL(a.TIPO_OS_CODIGO, ''''),
            tpos_ds = ISNULL(a.TIPO_OS_DESCRICAO, ''''),
            tpos_ativa = '''',
            TipoOS_Codigo = ''S/DePara'',
            TipoOS_Descricao = ''S/DePara'',
            TipoOS_Sigla = ''S/DePara'',
            Origem = ''Ficha_Seguimento''
        FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG a
        WHERE NOT EXISTS (
            SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoOS_DePara b
            WHERE b.tpos_cd = a.TIPO_OS_CODIGO COLLATE DATABASE_DEFAULT
              AND b.tpos_ds = a.TIPO_OS_DESCRICAO COLLATE DATABASE_DEFAULT
        )
    END
'
EXEC sp_executesql @CMD

SELECT @CMD = '
    IF EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects WHERE type = ''U'' AND name = ''Ficha_Srv_MG'')
    BEGIN
        INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoOS_DePara
            (tpos_cd, tpos_ds, tpos_ativa, TipoOS_Codigo, TipoOS_Descricao, TipoOS_Sigla, Origem)
        SELECT DISTINCT
            tpos_cd = ISNULL(a.TIPO_OS_CODIGO, ''''),
            tpos_ds = ISNULL(a.TIPO_OS_DESCRICAO, ''''),
            tpos_ativa = '''',
            TipoOS_Codigo = ''S/DePara'',
            TipoOS_Descricao = ''S/DePara'',
            TipoOS_Sigla = ''S/DePara'',
            Origem = ''Ficha_Seguimento''
        FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG a
        WHERE NOT EXISTS (
            SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoOS_DePara b
            WHERE b.tpos_cd = a.TIPO_OS_CODIGO COLLATE DATABASE_DEFAULT
              AND b.tpos_ds = a.TIPO_OS_DESCRICAO COLLATE DATABASE_DEFAULT
        )
    END
'
EXEC sp_executesql @CMD

SELECT @CMD = '
    UPDATE a
    SET a.TipoOS_Codigo = b.TipoOS_Codigo,
        a.TipoOS_Descricao = b.TipoOS_Descricao,
        a.TipoOS_Sigla = b.TipoOS_Sigla
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoOS_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.TipoOS b
        ON b.TipoOS_Sigla = a.tpos_cd
       AND b.TipoOS_Descricao = a.tpos_ds COLLATE Latin1_General_CI_AI
    WHERE a.TipoOS_Codigo = ''S/DePara''
'
EXEC sp_executesql @CMD
GO
-- <<< FIM: up_01_Fseg_DePara_TipoOS.sql
GO
