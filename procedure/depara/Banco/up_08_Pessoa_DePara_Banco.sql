-- =============================================================================
-- Layout/trigger: forn_cli_dados_bancarios (pos-importacao)
-- Destino : Banco_DePara
-- Procedure: up_08_Pessoa_DePara_Banco
-- Params: @BancoDadosGX, @BancoWF
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================

USE [DadosGX_Simulacao];  -- ALTERE
GO

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
