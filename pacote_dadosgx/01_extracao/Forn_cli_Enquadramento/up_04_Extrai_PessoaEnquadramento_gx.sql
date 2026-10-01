-- =============================================================================
-- Layout: Forn_cli_Enquadramento.txt
-- Staging : Arquivo_Forn_Cli_Enquadramento_Tratado
-- Destino : PessoaEnquadramento_MG
-- Procedure: up_04_Extrai_PessoaEnquadramento_gx (@BancoDadosGX)
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================

USE [DadosGX_SeuProjeto];  -- ALTERE para o nome real do banco DadosGX
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_04_Extrai_PessoaEnquadramento_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_04_Extrai_PessoaEnquadramento_gx;
GO

CREATE PROCEDURE dbo.up_04_Extrai_PessoaEnquadramento_gx
	@BancoDadosGX		VARCHAR(MAX)
	
AS

DECLARE @CMD NVARCHAR(MAX)
-- ==========================================================================

	IF ( NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX) )
		BEGIN 
			PRINT 'O < '+ @BancoDadosGX +' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
			RETURN
		END
	IF ( NOT EXISTS (SELECT 1 FROM SYS.OBJECTS WHERE NAME = 'Arquivo_Forn_Cli_Enquadramento_Tratado') )
		BEGIN 
			PRINT 'O < ARQUIVO > INFORMADO NAO EXISTE NESTE BANCO '+ @BancoDadosGX +'!'
			RETURN
		END

-- ==========================================================================================

--DROP TABLE dbo.PessoaEnquadramento_MG

PRINT '==========================================================================================' 
PRINT ' PessoaEnquadramento - Atualiza CPF_CNPJ ' 
PRINT '=========================================================================================='

SELECT @CMD = '

	UPDATE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Arquivo_Forn_Cli_Enquadramento_Tratado SET
		CPF_CNPJ = rtrim(ltrim(dbo.FN_RemoveCaracteresNaoInteiros(CPF_CNPJ)))

'

EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' Cria a cópia do Arquivo_Forn_Cli_Enquadramento_Tratado para Migração' 
PRINT '=========================================================================================='

SELECT @CMD = '

	IF(NOT EXISTS(SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects WHERE type = ''U'' AND name =''PessoaEnquadramento_MG''))
	BEGIN
		SELECT a.*,1 as Flag INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaEnquadramento_MG FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Arquivo_Forn_Cli_Enquadramento_Tratado a

		ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaEnquadramento_MG	ADD	Pessoa_DocIdentificador	varchar (20)	NULL
		ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaEnquadramento_MG	ADD	Municipio_Codigo	int
		ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaEnquadramento_MG	ADD	Estado_Codigo	varchar(10)
		ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaEnquadramento_MG	ADD	Data_Cadastro	date
		ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaEnquadramento_MG	ADD	Ocorrencia	VARCHAR(500)
	END

'

EXEC sp_executesql @CMD

PRINT '==========================================================================================' 
PRINT ' Atualiza Flag = 0 na PessoaEnquadramento_MG CPF/CNPJ em BRANCO ' 
PRINT '=========================================================================================='

SELECT @CMD = '

	UPDATE a SET a.Ocorrencia = '''' FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaEnquadramento_MG a

	UPDATE a  
	SET	a.Ocorrencia = CASE
						WHEN CPF_CNPJ IS NULL OR CPF_CNPJ = '''' 
						THEN a.Ocorrencia + '' | CPF CNPJ está vazio''
						ELSE a.Ocorrencia
					END,
		a.Flag = CASE
					WHEN CPF_CNPJ IS NULL OR CPF_CNPJ = '''' THEN 0
					ELSE 1
				END
	FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaEnquadramento_MG a		

'

EXEC sp_executesql @CMD

PRINT '==========================================================================================' 
PRINT ' Atualiza Flag = 0 Pessoa já cadastrado em WF-Produção ' 
PRINT '=========================================================================================='

SELECT @CMD = '

	UPDATE a SET
		a.Flag = 0
		,a.Ocorrencia = a.Ocorrencia + '' Cadastrado da Pessoa no WF-Produção.''
	FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaEnquadramento_MG a
	INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_EmProducao b ON a.CPF_CNPJ = b.CPF_CNPJ

'

EXEC sp_executesql @CMD

PRINT '==========================================================================================' 
PRINT ' Atualiza Flag na Tabela PessoaEnquadramento_MG ' 
PRINT '=========================================================================================='

SELECT @CMD = '

	UPDATE a SET
		a.Flag = 0
		,a.Ocorrencia = a.Ocorrencia + '' | CPF CNPJ não existe na Pessoa_MG.''
	FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaEnquadramento_MG a	
	WHERE 
		1=1 AND
		NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_MG b WHERE a.CPF_CNPJ = b.CPF_CNPJ)

'

EXEC sp_executesql @CMD

PRINT '==========================================================================================' 
PRINT ' Ajusta Pessoa_DocIdentificador na Tabela PessoaEnquadramento_MG ' 
PRINT '=========================================================================================='

SELECT @CMD = '

	UPDATE a 
	SET	a.Pessoa_DocIdentificador = CASE 
								WHEN len(RTRIM(LTRIM(CPF_CNPJ))) < 11 
									THEN replicate(''0'',11 - len(RTRIM(LTRIM(CPF_CNPJ)))) + RTRIM(LTRIM(CPF_CNPJ)) 
								WHEN len(RTRIM(LTRIM(CPF_CNPJ))) > 11 and len(RTRIM(LTRIM(CPF_CNPJ))) < 14 
									THEN replicate(''0'',14 - len(RTRIM(LTRIM(CPF_CNPJ)))) + RTRIM(LTRIM(CPF_CNPJ)) 
								ELSE RTRIM(LTRIM(CPF_CNPJ)) 
							END
	FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaEnquadramento_MG a
	WHERE
		a.Flag = 1
'

EXEC sp_executesql @CMD

PRINT '==========================================================================================' 
PRINT ' Atualiza DATA_CADASTRO na Tabela PessoaEnquadramento_MG ' 
PRINT '=========================================================================================='

SELECT @CMD = '

	UPDATE a SET
		a.DATA_CADASTRO = CASE WHEN b.DATA_CADASTRO IS NULL OR LTRIM(RTRIM(CONVERT(varchar(30), b.DATA_CADASTRO, 23))) = '''' THEN CONVERT(varchar(10), CAST(GETDATE() AS date), 23) ELSE COALESCE(CONVERT(varchar(10), TRY_CONVERT(date, REPLACE(REPLACE(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), b.DATA_CADASTRO, 23))), 10), ''-'', ''/''), ''.'', ''/''), 103), 23),CONVERT(varchar(10), TRY_CONVERT(date, REPLACE(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), b.DATA_CADASTRO, 23))), 10), ''/'', ''-''), 23), 23),CONVERT(varchar(10), TRY_CONVERT(date, LTRIM(RTRIM(CONVERT(varchar(30), b.DATA_CADASTRO, 23))), 112), 23),CASE WHEN LEN(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), b.DATA_CADASTRO, 23))), 10)) <= 8 THEN CONVERT(varchar(10), TRY_CONVERT(date, REPLACE(REPLACE(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), b.DATA_CADASTRO, 23))), 10), ''-'', ''/''), ''.'', ''/''), 3), 23) END,''1900-01-01'') END
	FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaEnquadramento_MG a	
	INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_MG b ON a.CPF_CNPJ = b.CPF_CNPJ
	WHERE
		a.Flag = 1

'

EXEC sp_executesql @CMD

GO
