-- =============================================================================
-- Layout: Forn_cli_Contato.txt
-- Staging : Arquivo_Forn_cli_Contato_Tratado
-- Destino : PessoaContato_MG
-- Procedure: up_06_Extrai_PessoaContato_gx (@BancoDadosGX)
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================

USE [DadosGX_SeuProjeto];  -- ALTERE para o nome real do banco DadosGX
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_06_Extrai_PessoaContato_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_06_Extrai_PessoaContato_gx;
GO

CREATE PROCEDURE dbo.up_06_Extrai_PessoaContato_gx
	@BancoDadosGX		VARCHAR(MAX)
	
AS

DECLARE @CMD NVARCHAR(MAX)
-- ==========================================================================

	IF ( NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX) )
		BEGIN 
			PRINT 'O < '+ @BancoDadosGX +' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
			RETURN
		END
	IF ( NOT EXISTS (SELECT 1 FROM SYS.OBJECTS WHERE NAME = 'Arquivo_Forn_cli_Contato_Tratado') )
		BEGIN 
			PRINT 'O < ARQUIVO > INFORMADO NAO EXISTE NESTE BANCO '+ @BancoDadosGX +'!'
			RETURN
		END

-- ==========================================================================================

--DROP TABLE dbo.PessoaContato_MG

PRINT '==========================================================================================' 
PRINT ' PessoaContato - Atualiza CPF_CNPJ ' 
PRINT '=========================================================================================='

SELECT @CMD = '

	UPDATE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Arquivo_Forn_cli_Contato_Tratado SET
		CPF_CNPJ = rtrim(ltrim(dbo.FN_RemoveCaracteresNaoInteiros(CPF_CNPJ)))

'

EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' Cria a cópia do Arquivo_Forn_cli_Contato_Tratado para Migração' 
PRINT '=========================================================================================='

SELECT @CMD = '

	IF(NOT EXISTS(SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects WHERE type = ''U'' and name =''PessoaContato_MG''))
	BEGIN
		SELECT a.*,1 AS Flag into ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaContato_MG FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Arquivo_Forn_cli_Contato_Tratado a

		ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaContato_MG	ADD	Pessoa_DocIdentificador	varchar (20)	NULL
		ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaContato_MG	ADD	Ocorrencia	VARCHAR(500)
	END

'

EXEC sp_executesql @CMD

PRINT '==========================================================================================' 
PRINT ' Atualiza Flag = 0 na PessoaContato_MG CPF/CNPJ em BRANCO ' 
PRINT '=========================================================================================='

SELECT @CMD = '

	UPDATE a SET a.Ocorrencia = '''' FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaContato_MG a

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
	FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaContato_MG a		

'

EXEC sp_executesql @CMD

PRINT '==========================================================================================' 
PRINT ' Atualiza Flag = 0 Pessoa já cadastrado em WF-Produção ' 
PRINT '=========================================================================================='

SELECT @CMD = '

	UPDATE a SET
		a.Flag = 0
		,a.Ocorrencia = a.Ocorrencia + '' Cadastrado da Pessoa no WF-Produção.''
	FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaContato_MG a
	INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_EmProducao b ON a.CPF_CNPJ = b.CPF_CNPJ

'

EXEC sp_executesql @CMD

PRINT '==========================================================================================' 
PRINT ' Atualiza Flag = 0 na Tabela PessoaContato_MG CPF/CNPJ não existe na Pessoa_MG' 
PRINT '=========================================================================================='

SELECT @CMD = '

	UPDATE a SET
		a.Flag = 0
		,a.Ocorrencia = a.Ocorrencia + '' | CPF CNPJ não existe na Pessoa_MG.''
	FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaContato_MG a	
	WHERE 
		1=1 AND
		NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_MG b WHERE a.CPF_CNPJ = b.CPF_CNPJ)

'

EXEC sp_executesql @CMD

PRINT '==========================================================================================' 
PRINT ' DESCARTA CONTATOS INVÁLIDOS/SEM NOME Flag = 0 na Tabela PessoaContato_MG ' 
PRINT '=========================================================================================='

SELECT @CMD = '

	UPDATE a SET
		a.Flag = 0
		,a.Ocorrencia = a.Ocorrencia + '' | Contatos Inválidos/Sem NOME.''
	FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaContato_MG a	
	WHERE 
		a.NOME_CONTATO = ''''

'

EXEC sp_executesql @CMD

PRINT '==========================================================================================' 
PRINT ' Ajusta DT_ANIVER na Tabela PessoaContato_MG' 
PRINT '=========================================================================================='

SELECT @CMD = '

	UPDATE a SET
		a.DT_ANIVER = COALESCE(CONVERT(varchar(10), TRY_CONVERT(date, REPLACE(REPLACE(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), a.DT_ANIVER, 23))), 10), ''-'', ''/''), ''.'', ''/''), 103), 23),CONVERT(varchar(10), TRY_CONVERT(date, REPLACE(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), a.DT_ANIVER, 23))), 10), ''/'', ''-''), 23), 23),CONVERT(varchar(10), TRY_CONVERT(date, LTRIM(RTRIM(CONVERT(varchar(30), a.DT_ANIVER, 23))), 112), 23),CASE WHEN LEN(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), a.DT_ANIVER, 23))), 10)) <= 8 THEN CONVERT(varchar(10), TRY_CONVERT(date, REPLACE(REPLACE(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), a.DT_ANIVER, 23))), 10), ''-'', ''/''), ''.'', ''/''), 3), 23) END,''1900-01-01'')
	FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaContato_MG a
	WHERE 
		a.Flag = 1
'							   

EXEC sp_executesql @CMD

PRINT '==========================================================================================' 
PRINT ' Ajusta EMAIL na Tabela PessoaContato_MG' 
PRINT '=========================================================================================='

SELECT @CMD = '

	UPDATE a SET
		a.EMAIL = ISNULL(LEFT(LTRIM(RTRIM(a.EMAIL)),150),'''')
	FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaContato_MG a
	WHERE 
		a.Flag = 1

	UPDATE a SET
		a.EMAIL_ALTERNATIVO = ISNULL(LEFT(LTRIM(RTRIM(a.EMAIL_ALTERNATIVO)),150),'''')
	FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaContato_MG a
	WHERE 
		a.Flag = 1
 
'

EXEC sp_executesql @CMD

PRINT '==========================================================================================' 
PRINT ' Ajusta Atualiza CPF_CNPJ_CONTATO na Tabela PessoaContato_MG' 
PRINT '=========================================================================================='

SELECT @CMD = '

	UPDATE a
	SET
		a.CPF_CNPJ_CONTATO = rtrim(ltrim(dbo.FN_RemoveCaracteresNaoInteiros(a.CPF_CNPJ_CONTATO)))
	FROM
		' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaContato_MG a
	WHERE
		a.Flag = 1 AND
		a.CPF_CNPJ_CONTATO != ''''
'

EXEC sp_executesql @CMD

PRINT '==========================================================================================' 
PRINT ' Ajusta Pessoa_DocIdentificador na Tabela PessoaContato_MG ' 
PRINT '=========================================================================================='

SELECT @CMD = '

	UPDATE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaContato_MG SET
	Pessoa_DocIdentificador = CASE 
								WHEN len(RTRIM(LTRIM(CPF_CNPJ))) < 11 
									THEN replicate(''0'',11 - len(RTRIM(LTRIM(CPF_CNPJ)))) + RTRIM(LTRIM(CPF_CNPJ)) 
								WHEN len(RTRIM(LTRIM(CPF_CNPJ))) > 11 and len(RTRIM(LTRIM(CPF_CNPJ))) < 14 
									THEN replicate(''0'',14 - len(RTRIM(LTRIM(CPF_CNPJ)))) + RTRIM(LTRIM(CPF_CNPJ)) 
								ELSE RTRIM(LTRIM(CPF_CNPJ)) 
							END
	WHERE
		Flag = 1
'

EXEC sp_executesql @CMD

GO
