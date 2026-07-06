-- =============================================================================
-- Layout/trigger: forn_cli_endereco (pos-importacao)
-- Destino : Estado_DePara, Pais_DePara
-- Procedure: up_07_Pessoa_DePara_Estado_Pais
-- Params: @BancoDadosGX, @BancoWF
-- =============================================================================

USE [DadosGX_Simulacao];  -- ALTERE
GO

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
