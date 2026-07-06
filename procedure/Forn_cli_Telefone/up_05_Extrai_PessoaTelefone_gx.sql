-- =============================================================================
-- Layout: Forn_cli_Telefone.txt
-- Staging : Arquivo_Forn_Cli_Telefone_Tratado
-- Destino : PessoaTelefone_MG
-- Procedure: up_05_Extrai_PessoaTelefone_gx (@BancoDadosGX, @DDDPadrao char(2))
-- =============================================================================

USE [DadosGX_Simulacao];  -- ALTERE
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_05_Extrai_PessoaTelefone_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_05_Extrai_PessoaTelefone_gx;
GO

CREATE PROCEDURE dbo.up_05_Extrai_PessoaTelefone_gx
	@BancoDadosGX		VARCHAR(MAX),
	@DDDPadrao			char(2)
AS

DECLARE @CMD NVARCHAR(MAX)
-- ==========================================================================

	IF ( NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX) )
		BEGIN 
			PRINT 'O < '+ @BancoDadosGX +' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
			RETURN
		END
	IF ( NOT EXISTS (SELECT 1 FROM SYS.OBJECTS WHERE NAME = 'Arquivo_Forn_Cli_Telefone_Tratado') )
		BEGIN 
			PRINT 'O < ARQUIVO > INFORMADO NAO EXISTE NESTE BANCO '+ @BancoDadosGX +'!'
			RETURN
		END

-- ==========================================================================================

--DROP TABLE dbo.PessoaTelefone_MG

PRINT '==========================================================================================' 
PRINT ' Extrai PessoaTelefone - Atualiza CPF_CNPJ ' 
PRINT '=========================================================================================='

SELECT @CMD = '

	UPDATE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Arquivo_Forn_Cli_Telefone_Tratado SET
		CPF_CNPJ = rtrim(ltrim(dbo.FN_RemoveCaracteresNaoInteiros(CPF_CNPJ)))

'

EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' Cria a cópia do Arquivo_Forn_Cli_Telefone_Tratado para Migração' 
PRINT '=========================================================================================='
	
SELECT @CMD = '

	IF(NOT EXISTS(SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects WHERE type = ''U'' AND name =''PessoaTelefone_MG''))
	BEGIN
		SELECT A.*,1 AS Flag INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaTelefone_MG FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Arquivo_Forn_Cli_Telefone_Tratado a
	
		ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaTelefone_MG	ADD	Pessoa_DocIdentificador	VARCHAR(20)	NULL
		ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaTelefone_MG	ADD	Ocorrencia	VARCHAR(500)	
	
	END
'

EXEC sp_executesql @CMD

PRINT '==========================================================================================' 
PRINT ' Atualiza Flag=0 na Tabela PessoaTelefone_MG CPF/CNPJ Telefone em BRANCO ' 
PRINT '=========================================================================================='

SELECT @CMD = '

	UPDATE a SET a.Ocorrencia = '''' FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaTelefone_MG a

	UPDATE a SET 
	a.Ocorrencia = CASE
					WHEN (NULLIF(DDD_FONE1, '''') IS NULL AND NULLIF(NUMERO_FONE1, '''') IS NULL) AND 
						 (NULLIF(DDD_FONE2, '''') IS NULL AND NULLIF(NUMERO_FONE2, '''') IS NULL) AND 
						 (NULLIF(DDD_FONE3, '''') IS NULL AND NULLIF(NUMERO_FONE3, '''') IS NULL) 
					THEN ''| CPF CNPJ não tem nenhum telefone válido''
					ELSE a.Ocorrencia
				END +
				CASE
					WHEN CPF_CNPJ IS NULL OR CPF_CNPJ = ''''
					THEN '' | CPF CNPJ está vazio''
					ELSE a.Ocorrencia
				END,
	a.Flag = CASE
			WHEN (NULLIF(DDD_FONE1, '''') IS NULL AND NULLIF(NUMERO_FONE1, '''') IS NULL) AND 
				(NULLIF(DDD_FONE2, '''') IS NULL AND NULLIF(NUMERO_FONE2, '''') IS NULL) AND 
				(NULLIF(DDD_FONE3, '''') IS NULL AND NULLIF(NUMERO_FONE3, '''') IS NULL) OR
				(CPF_CNPJ IS NULL OR CPF_CNPJ = '''')
			THEN 0
				ELSE 1
			END
	FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaTelefone_MG a		

'

EXEC sp_executesql @CMD

PRINT '==========================================================================================' 
PRINT ' Atualiza Flag = 0 Pessoa já cadastrado em WF-Produção ' 
PRINT '=========================================================================================='

SELECT @CMD = '

	UPDATE a SET
		a.Flag = 0
		,a.Ocorrencia = a.Ocorrencia + '' Cadastrado da Pessoa no WF-Produção.''
	FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaTelefone_MG a
	INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_EmProducao b ON a.CPF_CNPJ = b.CPF_CNPJ

'

EXEC sp_executesql @CMD

PRINT '==========================================================================================' 
PRINT ' Atualiza Flag=0 na Tabela PessoaTelefone_MG CPF/CNPJ não existe na Pessoa_MG' 
PRINT '=========================================================================================='

SELECT @CMD = '

	UPDATE a SET
		a.Flag = 0
		,a.Ocorrencia = a.Ocorrencia + '' | CPF CNPJ não existe na Pessoa_MG.''
	FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaTelefone_MG a	
	WHERE 
		1=1 AND
		NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_MG b WHERE a.CPF_CNPJ = b.CPF_CNPJ)

'

EXEC sp_executesql @CMD

PRINT '==========================================================================================' 
PRINT ' Ajusta Pessoa_DocIdentificador na Tabela PessoaTelefone_MG ' 
PRINT '=========================================================================================='

SELECT @CMD = '

	UPDATE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaTelefone_MG SET
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

PRINT '=========================================================================================='
PRINT ' Removendo caracteres do Campo DDD/NUMERO FONE' 
PRINT '=========================================================================================='

SELECT @CMD = '

	UPDATE  a 
	SET 
		 a.DDD_FONE1	= left(dbo.FN_RemoveCaracteresNaoInteiros(DDD_FONE1),2)
		,a.NUMERO_FONE1 = dbo.FN_RemoveCaracteresNaoInteiros(NUMERO_FONE1)
		
		,a.DDD_FONE2	= left(dbo.FN_RemoveCaracteresNaoInteiros(DDD_FONE2),2)
		,a.NUMERO_FONE2 = dbo.FN_RemoveCaracteresNaoInteiros(NUMERO_FONE2)
		
		,a.DDD_FONE3	= left(dbo.FN_RemoveCaracteresNaoInteiros(DDD_FONE3),2)
		,a.NUMERO_FONE3 = dbo.FN_RemoveCaracteresNaoInteiros(NUMERO_FONE3)
		
		--,a.DDD_FONE4	= left(dbo.FN_RemoveCaracteresNaoInteiros(DDD_FONE4),2)
		--,a.NUMERO_FONE4 = dbo.FN_RemoveCaracteresNaoInteiros(NUMERO_FONE4)
		
		--,a.DDD_FONE5	= left(dbo.FN_RemoveCaracteresNaoInteiros(DDD_FONE5),2)
		--,a.NUMERO_FONE5 = dbo.FN_RemoveCaracteresNaoInteiros(NUMERO_FONE5)
	FROM 
		' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaTelefone_MG a
	WHERE 
		a.Flag = 1

'
EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' Ajusta DDD_FONE' 
PRINT '=========================================================================================='

SELECT @CMD = '

	UPDATE  a 
	SET 
		 a.DDD_FONE1	= CASE 
							WHEN (a.DDD_FONE1 IS NULL OR a.DDD_FONE1 = '''') THEN ''' + LTRIM(RTRIM(@DDDPadrao)) + ''' 
						ELSE a.DDD_FONE1 
						END		
		,a.DDD_FONE2	= CASE 
							WHEN (a.DDD_FONE2 IS NULL OR a.DDD_FONE2 = '''') THEN ''' + LTRIM(RTRIM(@DDDPadrao)) + '''
						ELSE a.DDD_FONE2 
						END	
		,a.DDD_FONE3	= CASE 
							WHEN (a.DDD_FONE3 IS NULL OR a.DDD_FONE3 = '''') THEN ''' + LTRIM(RTRIM(@DDDPadrao)) + ''' 
						ELSE a.DDD_FONE3 
						END		
		--,a.DDD_FONE4	= CASE 
		--					WHEN (a.DDD_FONE4 IS NULL OR a.DDD_FONE4 = '''') THEN ''' + LTRIM(RTRIM(@DDDPadrao)) + ''' 
		--				ELSE a.DDD_FONE4 
		--				END		
		--,a.DDD_FONE5	= CASE 
		--					WHEN (a.DDD_FONE5 IS NULL OR a.DDD_FONE5 = '''') THEN ''' + LTRIM(RTRIM(@DDDPadrao)) + ''' 
		--				ELSE a.DDD_FONE5 
		--				END		
	FROM
		' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaTelefone_MG a
	WHERE 
		a.Flag = 1

'
EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' Ajusta NUMERO_FONE' 
PRINT '=========================================================================================='

SELECT @CMD = '

	UPDATE  a 
	SET 
		 a.NUMERO_FONE1	= CASE
							WHEN a.NUMERO_FONE1 !='''' THEN LTRIM( REPLACE( REPLACE( LEFT(a.NUMERO_FONE1,14),''-'',''''),''.'','''') )
						END												    		 												 
		,a.NUMERO_FONE2	= CASE 											    		 												 
							WHEN a.NUMERO_FONE2 !='''' THEN	LTRIM( REPLACE( REPLACE( LEFT(a.NUMERO_FONE2,14),''-'',''''),''.'','''') )
						END												    		 												 
		,a.NUMERO_FONE3	= CASE											    		 												 
							WHEN a.NUMERO_FONE3 !='''' THEN	LTRIM( REPLACE( REPLACE( LEFT(a.NUMERO_FONE3,14),''-'',''''),''.'','''') )
						END
		--,a.NUMERO_FONE4	= CASE											    		 												 
		--					WHEN a.NUMERO_FONE4 !='''' THEN	LTRIM( REPLACE( REPLACE( LEFT(a.NUMERO_FONE4,14),''-'',''''),''.'','''') )
		--				END

		--,a.NUMERO_FONE5	= CASE											    		 												 
		--					WHEN a.NUMERO_FONE5 !='''' THEN	LTRIM( REPLACE( REPLACE( LEFT(a.NUMERO_FONE5,14),''-'',''''),''.'','''') )
		--				END

	FROM 
		' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaTelefone_MG a
	WHERE 
		a.Flag = 1

'
EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' Ajusta TIPO_FONE ' 
PRINT '=========================================================================================='

SELECT @CMD = '

	UPDATE  a 
	SET 
		 a.TIPO_FONE1	= CASE
							WHEN LEFT(a.NUMERO_FONE1,1) in (''9'',''8'',''7'') THEN 2
							WHEN LEFT(a.NUMERO_FONE1,1) in (''2'',''3'',''4'',''5'') THEN 1
							WHEN a.TIPO_FONE1 = '''' THEN 5 
						ELSE a.TIPO_FONE1 
						END
	FROM 
		' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaTelefone_MG a
	WHERE 
		a.Flag = 1 AND
		ISNUMERIC(a.TIPO_FONE1) = 0 

	UPDATE  a 
	SET 
		 a.TIPO_FONE2	= CASE
							WHEN LEFT(a.NUMERO_FONE2,1) in (''9'',''8'',''7'') THEN 2
							WHEN LEFT(a.NUMERO_FONE2,1) in (''2'',''3'',''4'',''5'') THEN 1
							WHEN a.TIPO_FONE2 = '''' THEN 5 
						ELSE a.TIPO_FONE2 
						END
	FROM 
		' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaTelefone_MG a
	WHERE 
		a.Flag = 1 AND
		ISNUMERIC(a.TIPO_FONE2) = 0

	UPDATE  a 
	SET 
		 a.TIPO_FONE3	= CASE
							WHEN LEFT(a.NUMERO_FONE3,1) in (''9'',''8'',''7'') THEN 2
							WHEN LEFT(a.NUMERO_FONE3,1) in (''2'',''3'',''4'',''5'') THEN 1
							WHEN a.TIPO_FONE3 = '''' THEN 5 
						ELSE a.TIPO_FONE3 
						END
	FROM 
		' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaTelefone_MG a
	WHERE 
		a.Flag = 1 AND
		ISNUMERIC(a.TIPO_FONE3) = 0 
'
EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' Ajusta NUMERO_FONE para CELULAR ' 
PRINT '=========================================================================================='

SELECT @CMD = '

	UPDATE  a 
	SET 
		 a.NUMERO_FONE1	= ''9'' + a.NUMERO_FONE1
	FROM 
		' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaTelefone_MG a
	WHERE 
		a.Flag = 1 AND
		LEN(a.NUMERO_FONE1) = 8 AND
		a.TIPO_FONE1 = 2

	UPDATE  a 
	SET 
		 a.NUMERO_FONE2	= ''9'' + a.NUMERO_FONE2
	FROM 
		' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaTelefone_MG a
	WHERE 
		a.Flag = 1 AND
		LEN(a.NUMERO_FONE2) = 8 AND
		a.TIPO_FONE2 = 2

	UPDATE  a 
	SET 
		 a.NUMERO_FONE3	= ''9'' + a.NUMERO_FONE3
	FROM 
		' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaTelefone_MG a
	WHERE 
		a.Flag = 1 AND
		LEN(a.NUMERO_FONE1) = 8 AND
		a.TIPO_FONE3 = 2

'
EXEC sp_executesql @CMD

GO
