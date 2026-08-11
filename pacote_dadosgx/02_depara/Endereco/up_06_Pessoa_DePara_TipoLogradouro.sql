-- =============================================================================
-- Layout/trigger: forn_cli_endereco (pos-importacao)
-- Destino : TipoLogradouro_DePara
-- Procedure: up_06_Pessoa_DePara_TipoLogradouro
-- Params: @BancoDadosGX, @BancoWF
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================

USE [DadosGX_SeuProjeto];  -- ALTERE para o nome real do banco DadosGX
GO

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
