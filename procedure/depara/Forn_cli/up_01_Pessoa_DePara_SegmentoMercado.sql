-- =============================================================================
-- Layout/trigger: forn_cli (pos-importacao)
-- Destino : SegmentoMercado_DePara
-- Procedure: up_01_Pessoa_DePara_SegmentoMercado
-- Params: @BancoDadosGX, @BancoWF
-- =============================================================================

USE [DadosGX_Simulacao];  -- ALTERE
GO

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
