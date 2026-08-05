-- =============================================================================
-- Layout/trigger: forn_cli (pos-importacao)
-- Destino : Escolaridade_DePara
-- Procedure: up_02_Pessoa_DePara_Escolaridade
-- Params: @BancoDadosGX, @BancoWF
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================

USE [DadosGX_Simulacao];  -- ALTERE
GO

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
