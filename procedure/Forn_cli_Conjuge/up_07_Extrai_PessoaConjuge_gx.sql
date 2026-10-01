-- =============================================================================
-- Layout: Forn_cli_Conjuge.txt
-- Staging : Arquivo_Forn_cli_Conjuge_Tratado
-- Destino : PessoaConjuge_MG
-- Procedure: up_07_Extrai_PessoaConjuge_gx (@BancoDadosGX)
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================

USE [DadosGX_Simulacao];  -- ALTERE
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_07_Extrai_PessoaConjuge_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_07_Extrai_PessoaConjuge_gx;
GO

CREATE PROCEDURE dbo.up_07_Extrai_PessoaConjuge_gx
	@BancoDadosGX		VARCHAR(MAX)
	
AS

DECLARE @CMD NVARCHAR(MAX)
-- ==========================================================================

	IF ( NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX) )
		BEGIN 
			PRINT 'O < '+ @BancoDadosGX +' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
			RETURN
		END
	IF ( NOT EXISTS (SELECT 1 FROM SYS.OBJECTS WHERE NAME = 'Arquivo_Forn_cli_Conjuge_Tratado') )
		BEGIN 
			PRINT 'O < ARQUIVO > INFORMADO NAO EXISTE NESTE BANCO '+ @BancoDadosGX +'!'
			RETURN
		END
-- ==========================================================================================

--DROP TABLE dbo.FichaCadastralConjuge_MG

PRINT '==========================================================================================' 
PRINT ' PessoaConjuge - Atualiza CPF_CNPJ ' 
PRINT '=========================================================================================='

SELECT @CMD = '

	UPDATE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Arquivo_Forn_cli_Conjuge_Tratado SET
		CPF_CNPJ = rtrim(ltrim(dbo.FN_RemoveCaracteresNaoInteiros(CPF_CNPJ)))

'

EXEC sp_executesql @CMD

PRINT '==========================================================================================' 
PRINT ' PessoaConjuge - Atualiza CPF_CNPJ ' 
PRINT '=========================================================================================='
	
SELECT @CMD = '
	
	IF(NOT EXISTS(SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects where type = ''U'' AND name =''FichaCadastralConjuge_MG''))
	BEGIN
		SELECT a.*,1 as Flag INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.FichaCadastralConjuge_MG FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Arquivo_Forn_cli_Conjuge_Tratado a

		ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.FichaCadastralConjuge_MG	ADD	Pessoa_DocIdentificador varchar (20)	null
		ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.FichaCadastralConjuge_MG	ADD	Ocorrencia	VARCHAR(500)
	END

'

EXEC sp_executesql @CMD

PRINT '==========================================================================================' 
PRINT ' Atualiza Flag = 0 na FichaCadastralConjuge_MG CPF/CNPJ em BRANCO ' 
PRINT '=========================================================================================='

SELECT @CMD = '

	UPDATE a SET a.Ocorrencia = '''' FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.FichaCadastralConjuge_MG a

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
	FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.FichaCadastralConjuge_MG a		

'

EXEC sp_executesql @CMD

PRINT '==========================================================================================' 
PRINT ' Atualiza Flag = 0 Pessoa já cadastrado em WF-Produção ' 
PRINT '=========================================================================================='

SELECT @CMD = '

	UPDATE a SET
		a.Flag = 0
		,a.Ocorrencia = a.Ocorrencia + '' Cadastrado da Pessoa no WF-Produção.''
	FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.FichaCadastralConjuge_MG a
	INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_EmProducao b ON a.CPF_CNPJ = b.CPF_CNPJ

'

EXEC sp_executesql @CMD

PRINT '==========================================================================================' 
PRINT ' Atualiza Flag na Tabela FichaCadastralConjuge_MG CPF/CNPJ não existe na Pessoa_MG' 
PRINT '=========================================================================================='

SELECT @CMD = '

	UPDATE A SET
		a.Flag = 0
		,a.Ocorrencia = a.Ocorrencia + '' | CPF CNPJ não existe na Pessoa_MG.''
	FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.FichaCadastralConjuge_MG a	
	WHERE 
		1=1 AND
		NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_MG b where a.CPF_CNPJ = b.CPF_CNPJ)

'

EXEC sp_executesql @CMD

PRINT '==========================================================================================' 
PRINT ' Ajusta DT_ANIVER_CONJUGE na Tabela FichaCadastralConjuge_MG' 
PRINT '=========================================================================================='

SELECT @CMD = '

	UPDATE a SET
		a.DT_ANIVER_CONJUGE = COALESCE(CONVERT(varchar(10), TRY_CONVERT(date, REPLACE(REPLACE(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), a.DT_ANIVER_CONJUGE, 23))), 10), ''-'', ''/''), ''.'', ''/''), 103), 23),CONVERT(varchar(10), TRY_CONVERT(date, REPLACE(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), a.DT_ANIVER_CONJUGE, 23))), 10), ''/'', ''-''), 23), 23),CONVERT(varchar(10), TRY_CONVERT(date, LTRIM(RTRIM(CONVERT(varchar(30), a.DT_ANIVER_CONJUGE, 23))), 112), 23),CASE WHEN LEN(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), a.DT_ANIVER_CONJUGE, 23))), 10)) <= 8 THEN CONVERT(varchar(10), TRY_CONVERT(date, REPLACE(REPLACE(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), a.DT_ANIVER_CONJUGE, 23))), 10), ''-'', ''/''), ''.'', ''/''), 3), 23) END,''1900-01-01'')
	FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.FichaCadastralConjuge_MG a
	WHERE 
		a.Flag = 1
'							   

EXEC sp_executesql @CMD

PRINT '==========================================================================================' 
PRINT ' Ajusta CPF_CNPJ_CONTATO na Tabela FichaCadastralConjuge_MG ' 
PRINT '=========================================================================================='

SELECT @CMD = '

	UPDATE a
	SET
		a.CPF_CNPJ_CONJUGE = rtrim(ltrim(dbo.FN_RemoveCaracteresNaoInteiros(a.CPF_CNPJ_CONJUGE)))
	FROM
		' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.FichaCadastralConjuge_MG a
	WHERE
		a.Flag = 1 AND
		a.CPF_CNPJ_CONJUGE != '''' 
'

EXEC sp_executesql @CMD

PRINT '==========================================================================================' 
PRINT ' Ajusta Pessoa_DocIdentificador na Tabela FichaCadastralConjuge_MG ' 
PRINT '=========================================================================================='

SELECT @CMD = '

	UPDATE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.FichaCadastralConjuge_MG SET
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
