-- =============================================================================
-- Layout: Forn_cli_Endereco.txt
-- Staging : Arquivo_Forn_Cli_Endereco_Tratado
-- Destino : PessoaEndereco_MG
-- Procedure: up_02_Extrai_PessoaEndereco_gx (@BancoDadosGX)
-- =============================================================================

USE [DadosGX_Simulacao];  -- ALTERE
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_02_Extrai_PessoaEndereco_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_02_Extrai_PessoaEndereco_gx;
GO

CREATE PROCEDURE dbo.up_02_Extrai_PessoaEndereco_gx
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
			WHERE type = ''U'' AND name = ''Arquivo_Forn_Cli_Endereco_Tratado''
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

--DROP TABLE dbo.PessoaEndereco_MG

PRINT '==========================================================================================' 
PRINT ' Extrai PessoaEndereco - Atualiza CPFCNPJ ' 
PRINT '=========================================================================================='

SELECT @CMD = '

	UPDATE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Arquivo_Forn_Cli_Endereco_Tratado SET
		CPF_CNPJ = RTRIM(LTRIM(' + QUOTENAME(LTRIM(RTRIM(@BancoDadosGX))) + '.dbo.fn_RemoveCaracteresNaoInteiros(CPF_CNPJ)))

'

EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' Cria a cópia do Arquivo_Forn_Cli_Endereco_tratado para Migração' 
PRINT '=========================================================================================='

SELECT @CMD = '

	IF(NOT EXISTS(SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects WHERE type = ''U'' AND name =''PessoaEndereco_MG''))
	BEGIN
		SELECT A.* INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaEndereco_MG FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Arquivo_Forn_Cli_Endereco_Tratado a WHERE 1=1
	END

'

EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' Adicionando colunas na tabela PessoaEndereco_MG ' 
PRINT '=========================================================================================='

SELECT @CMD = '

	IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col, ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj 
					WHERE col.object_id = obj.object_id AND col.name = ''Pessoa_DocIdentificador'' AND obj.name = ''PessoaEndereco_MG''))
		BEGIN 
			ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaEndereco_MG add Pessoa_DocIdentificador varchar (20) null
		END

	IF NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col, ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj 
					WHERE col.object_id = obj.object_id AND col.name = ''Municipio_Codigo'' AND obj.name = ''PessoaEndereco_MG'')
		BEGIN  
			ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaEndereco_MG add Municipio_Codigo smallint NULL
		END 

	IF NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col, ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj 
					WHERE col.object_id = obj.object_id AND col.name = ''Estado_Codigo'' AND obj.name = ''PessoaEndereco_MG'')
		BEGIN
			ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaEndereco_MG add Estado_Codigo Char(2) NULL
		END

	IF NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col, ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj 
					WHERE col.object_id = obj.object_id AND col.name = ''Pais_Codigo'' AND obj.name = ''PessoaEndereco_MG'')
		BEGIN
			ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaEndereco_MG add Pais_Codigo smallint
		END

	IF NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col, ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj 
					WHERE col.object_id = obj.object_id AND col.name = ''TipoLogradouro_Codigo'' AND obj.name = ''PessoaEndereco_MG'')
		BEGIN
			ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaEndereco_MG add TipoLogradouro_Codigo smallint
		END
	
	IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col, ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj 
					WHERE col.object_id = obj.object_id AND col.name = ''Ocorrencia'' AND obj.name = ''PessoaEndereco_MG''))
		BEGIN 
			ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaEndereco_MG	ADD	Ocorrencia				VARCHAR(500)
		END
'

EXEC sp_executesql @CMD

PRINT '==========================================================================================' 
PRINT ' Atualiza Flag = 0 na PessoaEndereco_MG CPF/CNPJ em BRANCO ' 
PRINT '=========================================================================================='

SELECT @CMD = '
	
	UPDATE a SET a.Ocorrencia = '''' FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaEndereco_MG a

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
	FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaEndereco_MG a		

'

EXEC sp_executesql @CMD

PRINT '==========================================================================================' 
PRINT ' Atualiza Flag = 0 Pessoa já cadastrado em WF-Produção ' 
PRINT '=========================================================================================='

SELECT @CMD = '

	UPDATE a SET
		a.Flag = 0
		,a.Ocorrencia = a.Ocorrencia + '' Cadastrado da Pessoa no WF-Produção.''
	FROM 
		' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaEndereco_MG a
	INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_EmProducao b ON a.CPF_CNPJ = b.CPF_CNPJ

'

EXEC sp_executesql @CMD

PRINT '==========================================================================================' 
PRINT ' Atualiza Flag=0 na Tabela PessoaEndereco_MG CPF/CNPJ não existe na Pessoa_MG' 
PRINT '=========================================================================================='

SELECT @CMD = '

	UPDATE a 
	SET	a.Flag = 0
		,a.Ocorrencia = a.Ocorrencia + '' | CPF CNPJ não existe na Pessoa_MG.''
	FROM 
		' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaEndereco_MG a	
	WHERE 
		1=1 AND
		NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_MG b WHERE a.CPF_CNPJ = b.CPF_CNPJ)

'

EXEC sp_executesql @CMD

PRINT '==========================================================================================' 
PRINT ' Ajusta Pessoa_DocIdentificador na Tabela PessoaEndereco_MG ' 
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
	FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaEndereco_MG a
	WHERE
		a.Flag = 1
'

EXEC sp_executesql @CMD

PRINT '==========================================================================================' 
PRINT ' Remove as duplicidades de ENDEREÇO/TIPO ' 
PRINT '=========================================================================================='

SELECT @CMD = '

	IF(EXISTS(SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects WHERE name = ''Endereco_Duplicados'' AND type = ''U''))
		BEGIN 	
			DROP TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Endereco_Duplicados
		END

	SELECT 
		a.Pessoa_DocIdentificador
		,a.TIPO_ENDERECO
		,COUNT(a.Pessoa_DocIdentificador) AS QUANTIDADE 
		,MAX(a.IDTabela) AS ID
	INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Endereco_Duplicados
	FROM 
		' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaEndereco_MG a
	GROUP BY 
		a.Pessoa_DocIdentificador,
		a.TIPO_ENDERECO
	HAVING COUNT(a.Pessoa_DocIdentificador) > 1

	UPDATE a SET
		a.Flag = 0
		,a.Ocorrencia = a.Ocorrencia + '' | Duplicidades de Endereco. ''
	FROM 
		' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaEndereco_MG a
	INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Endereco_Duplicados b ON b.Pessoa_DocIdentificador = a.Pessoa_DocIdentificador collate database_default 
	WHERE 		
		a.IDtabela < b.ID

'

EXEC sp_executesql @CMD

PRINT '==========================================================================================' 
PRINT ' CRITICA - ENDEREÇOS FALTANDO COD_IBGE na Tabela PessoaEndereco_MG ' 
PRINT '=========================================================================================='

SELECT @CMD = '

	UPDATE a SET
		a.Ocorrencia = a.Ocorrencia + '' | Endereço COD_IBGE INVALIDO.''
	FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaEndereco_MG a	
	WHERE 
		ISNUMERIC(a.COD_IBGE) = 0 AND a.COD_IBGE = '''' 

'

EXEC sp_executesql @CMD

PRINT '==========================================================================================' 
PRINT ' CRITICA - ENDEREÇOS FALTANDO CEP na Tabela PessoaEndereco_MG ' 
PRINT '=========================================================================================='

SELECT @CMD = '

	UPDATE a SET
		a.Ocorrencia = a.Ocorrencia + '' | Endereço CEP em INVALIDO.''
	FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaEndereco_MG a	
	WHERE 
		ISNUMERIC(a.CEP) = 0 AND a.CEP = ''''

'

EXEC sp_executesql @CMD

PRINT '==========================================================================================' 
PRINT ' CRITICA - ENDEREÇOS FALTANDO NÚMERO na Tabela PessoaEndereco_MG ' 
PRINT '=========================================================================================='

SELECT @CMD = '

	UPDATE a SET
		a.Ocorrencia = a.Ocorrencia + '' | Endereço NUMERO INVALIDO.''
	FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaEndereco_MG a	
	WHERE 
		ISNUMERIC(a.NUMERO) = 0 AND a.NUMERO = ''''

'

EXEC sp_executesql @CMD

PRINT '==========================================================================================' 
PRINT ' CRITICA - ENDEREÇOS FALTANDO CIDADE na Tabela PessoaEndereco_MG ' 
PRINT '=========================================================================================='

SELECT @CMD = '

	UPDATE a SET
		a.Ocorrencia = a.Ocorrencia + '' | CIDADE em BRANCO.''
	FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaEndereco_MG a	
	WHERE 
		RTRIM( LTRIM(a.CIDADE) ) = ''''

'

EXEC sp_executesql @CMD

PRINT '==========================================================================================' 
PRINT ' CRITICA - ENDEREÇOS FALTANDO CIDADE e UF na Tabela PessoaEndereco_MG ' 
PRINT '=========================================================================================='

SELECT @CMD = '

	UPDATE a SET
		a.Ocorrencia = a.Ocorrencia + '' | ESTADO em BRANCO.''
	FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaEndereco_MG a	
	WHERE 
		RTRIM( LTRIM(a.ESTADO) ) = '''' 

'

EXEC sp_executesql @CMD

PRINT '==========================================================================================' 
PRINT ' Atualiza Flag=0 - ENDEREÇOS INVÁLIDOS na Tabela PessoaEndereco_MG ' 
PRINT '=========================================================================================='

SELECT @CMD = '

	UPDATE a SET
		a.Flag = 0
	FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaEndereco_MG a	
	WHERE 
		a.Ocorrencia = '' | Endereço COD_IBGE INVALIDO. | Endereço CEP em INVALIDO. | Endereço NUMERO INVALIDO. | CIDADE em BRANCO. | ESTADO em BRANCO.''

'

EXEC sp_executesql @CMD

PRINT '==========================================================================================' 
PRINT ' Ajusta CEP na Tabela PessoaEndereco_MG ' 
PRINT '=========================================================================================='

SELECT @CMD = '

	UPDATE a 
	SET	a.CEP = ISNULL(REPLACE(REPLACE(a.CEP,''-'',''''),''.'',''''),'''')
	FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaEndereco_MG a
	WHERE
		a.Flag = 1
'

EXEC sp_executesql @CMD

GO
