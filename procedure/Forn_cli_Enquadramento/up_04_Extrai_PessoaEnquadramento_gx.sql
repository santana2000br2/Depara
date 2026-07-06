-- =============================================================================
-- Layout: Forn_cli_Enquadramento.txt
-- Staging : Arquivo_Forn_Cli_Enquadramento_Tratado
-- Destino : PessoaEnquadramento_MG
-- Procedure: up_04_Extrai_PessoaEnquadramento_gx (@BancoDadosGX)
-- =============================================================================

USE [DadosGX_Simulacao];  -- ALTERE
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
	SELECT @CMD = N'
		IF NOT EXISTS (
			SELECT 1 FROM ' + QUOTENAME(LTRIM(RTRIM(@BancoDadosGX))) + N'.sys.objects
			WHERE type = ''U'' AND name = ''Arquivo_Forn_Cli_Enquadramento_Tratado''
		)
			SELECT @ok = 0
		ELSE
			SELECT @ok = 1
	'
	DECLARE @StagingOk BIT = 0
	EXEC sp_executesql @CMD, N'@ok BIT OUTPUT', @ok = @StagingOk OUTPUT
	IF @StagingOk = 0
		BEGIN
			PRINT 'O < ARQUIVO > INFORMADO NAO EXISTE NO BANCO '+ @BancoDadosGX +'!'
			RETURN
		END

-- ==========================================================================================

--DROP TABLE dbo.PessoaEnquadramento_MG

PRINT '==========================================================================================' 
PRINT ' PessoaEnquadramento - Atualiza CPF_CNPJ ' 
PRINT '=========================================================================================='

SELECT @CMD = '

	UPDATE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Arquivo_Forn_Cli_Enquadramento_Tratado SET
		CPF_CNPJ = RTRIM(LTRIM(' + QUOTENAME(LTRIM(RTRIM(@BancoDadosGX))) + '.dbo.fn_RemoveCaracteresNaoInteiros(CPF_CNPJ)))

'

EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' Cria a cópia do Arquivo_Forn_Cli_Enquadramento_Tratado para Migração' 
PRINT '=========================================================================================='

SELECT @CMD = '

	IF(NOT EXISTS(SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects WHERE type = ''U'' AND name =''PessoaEnquadramento_MG''))
	BEGIN
		SELECT a.* INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaEnquadramento_MG FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Arquivo_Forn_Cli_Enquadramento_Tratado a WHERE 1=1
	END

'

EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' Adicionando colunas na tabela PessoaEnquadramento_MG '
PRINT '=========================================================================================='

SELECT @CMD = '

	IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col, ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj
					WHERE col.object_id = obj.object_id AND col.name = ''Pessoa_DocIdentificador'' AND obj.name = ''PessoaEnquadramento_MG''))
		ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaEnquadramento_MG ADD Pessoa_DocIdentificador varchar(20) NULL

	IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col, ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj
					WHERE col.object_id = obj.object_id AND col.name = ''Municipio_Codigo'' AND obj.name = ''PessoaEnquadramento_MG''))
		ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaEnquadramento_MG ADD Municipio_Codigo int NULL

	IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col, ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj
					WHERE col.object_id = obj.object_id AND col.name = ''Estado_Codigo'' AND obj.name = ''PessoaEnquadramento_MG''))
		ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaEnquadramento_MG ADD Estado_Codigo varchar(10) NULL

	IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col, ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj
					WHERE col.object_id = obj.object_id AND col.name = ''Data_Cadastro'' AND obj.name = ''PessoaEnquadramento_MG''))
		ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaEnquadramento_MG ADD Data_Cadastro date NULL

	IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col, ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj
					WHERE col.object_id = obj.object_id AND col.name = ''Ocorrencia'' AND obj.name = ''PessoaEnquadramento_MG''))
		ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaEnquadramento_MG ADD Ocorrencia VARCHAR(500) NULL

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
		a.DATA_CADASTRO = cast(b.DATA_CADASTRO as date) 
	FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaEnquadramento_MG a	
	INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_MG b ON a.CPF_CNPJ = b.CPF_CNPJ
	WHERE
		a.Flag = 1

'

EXEC sp_executesql @CMD

GO
