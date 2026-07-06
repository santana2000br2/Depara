-- =============================================================================
-- Layout: 1 Forn_cli.txt
-- Staging : Arquivo_Forn_cli_Tratado
-- Destino : Pessoa_MG
-- Procedure: up_01_Extrai_Pessoa_gx (@BancoDadosGX, @BancoWF)
-- =============================================================================

USE [DadosGX_Simulacao];  -- ALTERE
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Extrai_Pessoa_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_01_Extrai_Pessoa_gx;
GO

CREATE PROCEDURE dbo.up_01_Extrai_Pessoa_gx
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
IF ( NOT EXISTS (SELECT 1 FROM SYS.OBJECTS WHERE NAME = 'Arquivo_Forn_cli_Tratado') )
	BEGIN 
		PRINT 'O < ARQUIVO > INFORMADO NAO EXISTE NESTE BANCO '+ @BancoDadosGX +'!'
		RETURN
	END
-- ==========================================================================================

--DROP TABLE dbo.Pessoa_MG

PRINT '==========================================================================================' 
PRINT ' Extrai_Pessoa - Atualiza CPFCNPJ ' 
PRINT '=========================================================================================='

SELECT @CMD = '

	UPDATE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Arquivo_Forn_cli_Tratado SET
		CPF_CNPJ = RTRIM(LTRIM(dbo.FN_RemoveCaracteresNaoInteiros(CPF_CNPJ)))

'

EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' Cria a cópia do Arquivo_Forn_cli_Tratado para Migração' 
PRINT '=========================================================================================='

SELECT @CMD = '

	IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects WHERE type = ''U'' AND name =''Pessoa_MG'') )
	BEGIN
		SELECT * INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_MG FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Arquivo_Forn_cli_Tratado WHERE 1=1
	END

'

EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' Adicionando colunas na tabela Pessoa_MG ' 
PRINT '=========================================================================================='

SELECT @CMD = '
		
	IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col, ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj 
					WHERE col.object_id = obj.object_id AND col.name = ''Pessoa_DocIdentificador'' AND obj.name = ''Pessoa_MG''))
	BEGIN 
		ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_MG add Pessoa_DocIdentificador varchar (20) null
	END 

	IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col, ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj 
					WHERE col.object_id = obj.object_id AND col.name = ''EstadoCivil_CodigoWF'' AND obj.name = ''Pessoa_MG''))
	BEGIN 
		ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_MG add EstadoCivil_CodigoWF smallint NULL
	END 

	IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col, ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj 
					WHERE col.object_id = obj.object_id AND col.name = ''Escolaridade_CodigoWF'' AND obj.name = ''Pessoa_MG''))
	BEGIN 
		ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_MG add Escolaridade_CodigoWF smallint NULL
	END 

	IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col, ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj 
					WHERE col.object_id = obj.object_id AND col.name = ''Profissao_CodigoWF'' AND obj.name = ''Pessoa_MG''))
	BEGIN 
		ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_MG add Profissao_CodigoWF smallint NULL
	END 

	IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col, ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj 
					WHERE col.object_id = obj.object_id AND col.name = ''Pessoa_PaisOrigemCod'' AND obj.name = ''Pessoa_MG''))
	BEGIN 
		ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_MG add Pessoa_PaisOrigemCod smallint NULL
	END 

	IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col, ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj 
					WHERE col.object_id = obj.object_id AND col.name = ''Pessoa_SegmentoMercado_Balcao'' AND obj.name = ''Pessoa_MG''))
	BEGIN 
		ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_MG add Pessoa_SegmentoMercado_Balcao smallint NULL
	END 

	IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col, ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj 
					WHERE col.object_id = obj.object_id AND col.name = ''Pessoa_SegmentoMercado_Oficina'' AND obj.name = ''Pessoa_MG''))
	BEGIN 
		ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_MG add Pessoa_SegmentoMercado_Oficina smallint NULL
	END 

	IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col, ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj 
					WHERE col.object_id = obj.object_id AND col.name = ''Pessoa_SegmentoMercado_Vendas'' AND obj.name = ''Pessoa_MG''))
	BEGIN 
		ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_MG add Pessoa_SegmentoMercado_Vendas smallint  NULL
	END

	IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col, ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj 
					WHERE col.object_id = obj.object_id AND col.name = ''PessoaRegraUso_Chave'' AND obj.name = ''Pessoa_MG''))
	BEGIN 
		ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_MG add PessoaRegraUso_Chave int  NULL
	END
	
	IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col, ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj 
					WHERE col.object_id = obj.object_id AND col.name = ''Pessoa_CodigoMyHonda'' AND obj.name = ''Pessoa_MG''))		--Ajuste para Integração MYHONDA
	BEGIN 
		ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_MG add Pessoa_CodigoMyHonda bigint  NULL
	END
	
	IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col, ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj 
					WHERE col.object_id = obj.object_id AND col.name = ''Pessoa_BloqueiaVendaTituloAtraso'' AND obj.name = ''Pessoa_MG''))
	BEGIN 
		ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_MG add Pessoa_BloqueiaVendaTituloAtraso smallint NULL
	END
	
	IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col, ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj 
					WHERE col.object_id = obj.object_id AND col.name = ''Pessoa_BloqueiaEntradaOficina'' AND obj.name = ''Pessoa_MG''))
	BEGIN 
		ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_MG add Pessoa_BloqueiaEntradaOficina smallint NULL
	END

	IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col, ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj 
					WHERE col.object_id = obj.object_id AND col.name = ''Ocorrencia'' AND obj.name = ''Pessoa_MG''))
	BEGIN 
		ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_MG	ADD	Ocorrencia	VARCHAR(500) null
	END
'

EXEC sp_executesql @CMD

PRINT '==========================================================================================' 
PRINT ' Atualiza Flag=0 na Tabela Pessoa_MG CPF/CNPJ em BRANCO ' 
PRINT '=========================================================================================='

SELECT @CMD = '

	UPDATE a SET a.Ocorrencia = '''' FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_MG a

	UPDATE a  
	SET	a.Ocorrencia = CASE
						WHEN a.CPF_CNPJ IS NULL OR a.CPF_CNPJ = '''' 
						THEN a.Ocorrencia + '' | CPF CNPJ está vazio''
						ELSE a.Ocorrencia
					END,
		a.Flag = CASE
					WHEN a.CPF_CNPJ IS NULL OR a.CPF_CNPJ = '''' THEN 0
					ELSE 1
				END
	FROM 
		' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_MG a		

'

EXEC sp_executesql @CMD

PRINT '==========================================================================================' 
PRINT ' Ajusta Pessoa_DocIdentificador na Tabela Pessoa_MG ' 
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
	FROM 
		' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_MG a
	WHERE
		a.Flag = 1
'

EXEC sp_executesql @CMD

PRINT '==========================================================================================' 
PRINT ' Ajusta TIPO na Tabela Pessoa_MG ' 
PRINT '=========================================================================================='

SELECT @CMD = '

	UPDATE a 
	SET
		a.Tipo = CASE 
					WHEN len(RTRIM(LTRIM(Pessoa_DocIdentificador))) <= 11 
						THEN ''F''
					WHEN len(RTRIM(LTRIM(Pessoa_DocIdentificador))) > 11 AND len(RTRIM(LTRIM(CPF_CNPJ))) <= 14 
						THEN ''J''
					ELSE RTRIM(LTRIM(Pessoa_DocIdentificador)) 
				END
		FROM
			' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_MG a
	WHERE
		a.Flag = 1	AND
		a.Tipo NOT IN (''F'',''J'',''C'',''M'',''T'',''S'',''G'',''E'')

'

EXEC sp_executesql @CMD

PRINT '==========================================================================================' 
PRINT ' Consulta Pessoa_DocIdentificador já cadastrados em WF-Produção ' 
PRINT '=========================================================================================='

SELECT @CMD = '
	
	IF(EXISTS(SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects WHERE name = ''Pessoa_EmProducao'' AND type = ''U''))
		BEGIN 	
			DROP TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_EmProducao
		END

	SELECT a.* 
	INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_EmProducao
	FROM  
		' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_MG a
	WHERE 			
		1=1 AND 
		EXISTS (SELECT 1 FROM '+LTRIM(RTRIM(@BancoWF))+'.dbo.Pessoa b
					WHERE 
						a.Pessoa_DocIdentificador = b.Pessoa_DocIdentificador collate database_Default)

	UPDATE a SET
		a.Flag = 0
		,a.Ocorrencia = a.Ocorrencia + '' Cadastrado em WF-Produção.''
	FROM  
		' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_MG a
	WHERE 			
		1=1 AND
		a.Flag = 1 AND 
		EXISTS (SELECT 1 FROM '+LTRIM(RTRIM(@BancoWF))+'.dbo.pessoa b
					WHERE 
						a.Pessoa_DocIdentificador =  b.Pessoa_DocIdentificador collate database_Default)
'

EXEC sp_executesql @CMD

PRINT '==========================================================================================' 
PRINT ' Remove as duplicidades de CPF da Pessoa ' 
PRINT '=========================================================================================='

SELECT @CMD = '

	IF(EXISTS(SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects WHERE name = ''Cliente_Duplicados'' AND type = ''U''))
		BEGIN 	
			DROP TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Cliente_Duplicados
		END

	SELECT 
		Pessoa_DocIdentificador, 
		COUNT(*) AS QUANTIDADE, 
		MAX(IDtabela) AS ID
	INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Cliente_Duplicados
	FROM 
		' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_MG
	GROUP BY Pessoa_DocIdentificador
	HAVING count(*) > 1

	UPDATE a SET
		a.Flag = 0
		,a.Ocorrencia = a.Ocorrencia + '' | Duplicidades de CPF/CNPJ.''
	FROM 
		' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_MG a
	INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Cliente_Duplicados b on a.Pessoa_DocIdentificador = b.Pessoa_DocIdentificador collate database_default 
	WHERE 		
		a.IDtabela < b.ID

'

EXEC sp_executesql @CMD

PRINT '==========================================================================================' 
PRINT ' Remove Pessoa com CPF_CNPJ NULL' 
PRINT '=========================================================================================='

SELECT @CMD = '

	IF(EXISTS(SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects WHERE name = ''CPFCNPJ_NULL'' AND type = ''U''))
		BEGIN 	
			DROP TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.CPFCNPJ_NULL
		END

	SELECT 
		IDtabela, 
		CODIGO_PESSOA, 
		NOME, 
		TIPO, 
		CPF_CNPJ, 
		Pessoa_DocIdentificador
	INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.CPFCNPJ_NULL
	FROM 
		' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_MG
	WHERE 
		Pessoa_DocIdentificador IS NULL OR Pessoa_DocIdentificador = ''''

	UPDATE a SET
		a.Flag = 0	
		,a.Ocorrencia = a.Ocorrencia + '' | Registros de CPF/CNPJ NULOS.''
	FROM 
		' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_MG a
	WHERE 
		a.Flag = 1
		and (a.Pessoa_DocIdentificador is null or a.Pessoa_DocIdentificador = '''')

'

EXEC sp_executesql @CMD

PRINT '==========================================================================================' 
PRINT ' Atualiza BLOQUEIA_VENDA e BLOQUEIA_OFICINA na Tabela Pessoa_MG ' 
PRINT '=========================================================================================='

SELECT @CMD = '

	UPDATE a SET
		a.Pessoa_BloqueiaVendaTituloAtraso = CASE
												WHEN RTRIM(LTRIM(a.BLOQUEIA_VENDA)) = ''N'' THEN 0
												WHEN RTRIM(LTRIM(a.BLOQUEIA_VENDA)) = ''S'' THEN 1
											ELSE
												ISNULL(BLOQUEIA_VENDA,0)
											END

		,a.Pessoa_BloqueiaEntradaOficina = CASE
												WHEN RTRIM(LTRIM(a.BLOQUEIA_OFICINA)) = ''N'' THEN 0
												WHEN RTRIM(LTRIM(a.BLOQUEIA_OFICINA)) = ''S'' THEN 1
											ELSE
												ISNULL(BLOQUEIA_OFICINA,0)
											END
	
	FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_MG a
	WHERE 
		a.Flag = 1
																						
'

EXEC sp_executesql @CMD

PRINT '==========================================================================================' 
PRINT ' Ajusta DT_ANIVER na Tabela Pessoa_MG' 
PRINT '=========================================================================================='

SELECT @CMD = '

	UPDATE a SET
		a.DT_ANIVER = CASE
						WHEN ISDATE(REPLACE(a.DT_ANIVER,''/'',''-'')) = 0 THEN ''1900-01-01'' 
						ELSE REPLACE(a.DT_ANIVER,''/'',''-'')  
					END
	FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_MG a
	WHERE 
		a.Flag = 1
'							   

EXEC sp_executesql @CMD

PRINT '==========================================================================================' 
PRINT ' Ajusta DATA_CADASTRO na Tabela Pessoa_MG' 
PRINT '=========================================================================================='

SELECT @CMD = '

	UPDATE a SET
		a.DATA_CADASTRO = CASE
							WHEN ISDATE(REPLACE(a.DATA_CADASTRO,''/'',''-'')) = 0	THEN	''1900-01-01''
							WHEN (DATA_CADASTRO IS NULL OR DATA_CADASTRO = '''')	THEN	CAST(GETDATE() as date) 
							ELSE REPLACE(a.DATA_CADASTRO,''/'',''-'') 
						END
	FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_MG a
	WHERE 
		a.Flag = 1
'

EXEC sp_executesql @CMD

PRINT '==========================================================================================' 
PRINT ' Ajusta E_MAIL na Tabela Pessoa_MG' 
PRINT '=========================================================================================='

SELECT @CMD = '

	UPDATE a SET
		a.E_MAIL = ISNULL(LEFT(LTRIM(RTRIM(a.E_MAIL)),150),'''')
	FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_MG a
	WHERE 
		a.Flag = 1

	UPDATE a SET
		a.EMAIL_ALTERNATIVO = ISNULL(LEFT(LTRIM(RTRIM(a.EMAIL_ALTERNATIVO)),150),'''')
	FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_MG a
	WHERE 
		a.Flag = 1
 
'

EXEC sp_executesql @CMD

PRINT '==========================================================================================' 
PRINT ' Ajusta LIM_CREDITO na Tabela Pessoa_MG' 
PRINT '=========================================================================================='

SELECT @CMD = '

	UPDATE a SET
		a.LIM_CREDITO = CASE 
							WHEN ISNUMERIC(a.LIM_CREDITO) = 0 THEN 0 
							ELSE TRY_CONVERT(FLOAT, REPLACE(a.LIM_CREDITO, '','', ''.''))  
						END
	FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_MG a
	WHERE 
		a.Flag = 1
'   

EXEC sp_executesql @CMD

PRINT '==========================================================================================' 
PRINT ' Ajusta LIM_CREDITO_VALIDADE na Tabela Pessoa_MG' 
PRINT '=========================================================================================='

SELECT @CMD = '

	UPDATE a SET
		a.LIM_CREDITO_VALIDADE = CASE 
									WHEN ISDATE(REPLACE(a.LIM_CREDITO_VALIDADE,''/'',''-'')) = 0 THEN ''1900-01-01'' 
									ELSE REPLACE(a.LIM_CREDITO_VALIDADE,''/'',''-'')  
								END
	FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_MG a
	WHERE 
		a.Flag = 1 AND
		ISNUMERIC(a.LIM_CREDITO) = 1
'

EXEC sp_executesql @CMD

PRINT '==========================================================================================' 
PRINT ' Atualiza CÓDIGO DMS ANTERIOR na Tabela Pessoa_MG para INTEGRAÇÃO MYHONDA' 
PRINT '=========================================================================================='

SELECT @CMD = '

	UPDATE a 
	SET a.Pessoa_CodigoMyHonda = CASE 
									WHEN ISNUMERIC(RTRIM( LTRIM( CODIGO_MYHONDA ) ) ) = 1 THEN CAST( RTRIM( LTRIM( CODIGO_MYHONDA ) ) AS BIGINT)
									ELSE 0
								END
	FROM 
		' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_MG a
	WHERE 
		a.Flag = 1
								
'

EXEC sp_executesql @CMD

SELECT @CMD = '

PRINT ''=========================================================================================='' 
PRINT '' Ajusta SEGMENTO MERCADO na Tabela Pessoa_MG '' 
PRINT ''==========================================================================================''

	UPDATE a SET
		a.SEGMENTO_OFICINA = CASE
								WHEN (a.SEGMENTO_OFICINA IS NULL OR a.SEGMENTO_OFICINA = '''') THEN ''CLIENTE OFICINA''
								ELSE a.SEGMENTO_OFICINA
							END,
		a.SEGMENTO_BALCAO = CASE
								WHEN (a.SEGMENTO_BALCAO IS NULL OR a.SEGMENTO_BALCAO = '''') THEN ''CLIENTE BALCÃO''
								ELSE a.SEGMENTO_BALCAO
							END,
		a.SEGMENTO_VENDAS = CASE
								WHEN (a.SEGMENTO_VENDAS IS NULL OR a.SEGMENTO_VENDAS = '''') THEN ''CLIENTE VENDAS''
								ELSE a.SEGMENTO_VENDAS
							END
	FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_MG a
	WHERE 
		a.Flag = 1
								
'
		
EXEC sp_executesql @CMD

GO
