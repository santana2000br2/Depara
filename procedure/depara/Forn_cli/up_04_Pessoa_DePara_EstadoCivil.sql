-- =============================================================================
-- Layout/trigger: forn_cli (pos-importacao)
-- Destino : EstadoCivil_DePara
-- Procedure: up_04_Pessoa_DePara_EstadoCivil
-- Params: @BancoDadosGX, @BancoWF
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================

USE [DadosGX_Simulacao];  -- ALTERE
GO

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
