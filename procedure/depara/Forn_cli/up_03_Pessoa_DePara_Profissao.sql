-- =============================================================================
-- Layout/trigger: forn_cli (pos-importacao)
-- Destino : Profissao_DePara
-- Procedure: up_03_Pessoa_DePara_Profissao
-- Params: @BancoDadosGX, @BancoWF
-- =============================================================================

USE [DadosGX_Simulacao];  -- ALTERE
GO

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
