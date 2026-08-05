-- =============================================================================
-- Layout/trigger: forn_cli_endereco (pos-importacao)
-- Destino : Municipio_DePara
-- Procedure: up_05_Pessoa_DePara_Municipio
-- Params: @BancoDadosGX, @BancoWF
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================

USE [DadosGX_Simulacao];  -- ALTERE
GO

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
