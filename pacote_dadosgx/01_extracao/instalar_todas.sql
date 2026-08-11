-- =============================================================================
-- INSTALAR — Procedures de Extração (DadosGX)
-- Instalador em T-SQL puro (NAO precisa SQLCMD Mode).
-- Antes de executar: substitua [DadosGX_SeuProjeto] pelo nome real do banco.
-- Gerado em: 10/08/2026 17:23
-- =============================================================================

USE [DadosGX_SeuProjeto];  -- ALTERE para o nome real do banco DadosGX
GO


-- >>> INICIO: 00_drop_procedures.sql
-- =============================================================================
-- DROP — Procedures de Extração (DadosGX)
-- Pacote DadosGX — gerar drop antes da reinstalacao
-- Gerado em: 10/08/2026 17:23
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Extrai_Financeiro_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_01_Extrai_Financeiro_gx];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Extrai_Pessoa_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_01_Extrai_Pessoa_gx];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_07_Extrai_PessoaConjuge_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_07_Extrai_PessoaConjuge_gx];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_06_Extrai_PessoaContato_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_06_Extrai_PessoaContato_gx];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_08_Extrai_PessoaBanco_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_08_Extrai_PessoaBanco_gx];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_03_Extrai_PessoaDoc_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_03_Extrai_PessoaDoc_gx];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_02_Extrai_PessoaEndereco_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_02_Extrai_PessoaEndereco_gx];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_04_Extrai_PessoaEnquadramento_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_04_Extrai_PessoaEnquadramento_gx];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_05_Extrai_PessoaTelefone_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_05_Extrai_PessoaTelefone_gx];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Extrai_Fseg_Cab_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_01_Extrai_Fseg_Cab_gx];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Extrai_Fseg_Prd_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_01_Extrai_Fseg_Prd_gx];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Extrai_Fseg_Srv_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_01_Extrai_Fseg_Srv_gx];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Extrai_MovimentoEstoque_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_01_Extrai_MovimentoEstoque_gx];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Extrai_ProdLocacao_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_01_Extrai_ProdLocacao_gx];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Extrai_Produto_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_01_Extrai_Produto_gx];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_02_Atualiza_Referencia_Produto_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_02_Atualiza_Referencia_Produto_gx];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_03_Trata_Duplicidade_Produto_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_03_Trata_Duplicidade_Produto_gx];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_04_Atualiza_Ocorrencia_Produto_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_04_Atualiza_Ocorrencia_Produto_gx];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Extrai_ProdutoEstoque_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_01_Extrai_ProdutoEstoque_gx];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Extrai_Veiculo_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_01_Extrai_Veiculo_gx];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_09_Extrai_Criticas' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_09_Extrai_Criticas];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_Replace_Name_DadosGx_Procedures' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_Replace_Name_DadosGx_Procedures];
GO
-- <<< FIM: 00_drop_procedures.sql
GO


-- >>> INICIO: up_Replace_Name_DadosGx_Procedures.sql
-- =============================================================================
-- Layout: Utilitario
-- Staging : (n/a)
-- Destino : (n/a)
-- Procedure: up_Replace_Name_DadosGx_Procedures (@BancoDadosGX)
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_Replace_Name_DadosGx_Procedures' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_Replace_Name_DadosGx_Procedures;
GO

CREATE PROCEDURE [dbo].up_Replace_Name_DadosGx_Procedures
	@BancoDadosGX VARCHAR(MAX)

AS

DECLARE @CMD NVARCHAR(MAX)

PRINT '==========================================================================================' 
PRINT ' PREPARAÇÃO 01 - (up_Replace_Name_DadosGx_Procedures)' 
PRINT '=========================================================================================='

SELECT @cmd = '

	IF EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects WHERE NAME = ''tb_temp_proc1'' AND TYPE = ''U'')
		BEGIN 
			DROP TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.tb_temp_proc1
		END 

	IF EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects WHERE NAME = ''tb_temp_proc2'' AND TYPE = ''U'')
		BEGIN 
			DROP TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.tb_temp_proc2
		END 

	IF EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects WHERE NAME = ''tb_temp_proc3'' AND TYPE = ''U'')
		BEGIN 
			DROP TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.tb_temp_proc3
		END 

'

EXEC sp_executesql @cmd

PRINT '==========================================================================================' 
PRINT ' PREPARAÇÃO 02 - (up_Replace_Name_DadosGx_Procedures)' 
PRINT '=========================================================================================='

SELECT @cmd = '

	CREATE TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.tb_temp_proc1 (object_id int, definition varchar(max));

	CREATE TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.tb_temp_proc2 (object_id int, definition varchar(max));

	CREATE TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.tb_temp_proc3 (object_id int, definition varchar(max));

'

EXEC sp_executesql @cmd

PRINT '==========================================================================================' 
PRINT ' PREPARAÇÃO 03 - (up_Replace_Name_DadosGx_Procedures)' 
PRINT '=========================================================================================='

SELECT @cmd = '

	INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.tb_temp_proc1
	SELECT
		object_id, definition
	FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.Sys.sql_modules
	WHERE 
		definition LIKE ''%DadosGx_ViaVR_AbrMai%'';                  -- TEXTO QUE CONSTA NA PROCEDURE
'

EXEC sp_executesql @cmd

PRINT '==========================================================================================' 
PRINT ' PREPARAÇÃO 04 - (up_Replace_Name_DadosGx_Procedures)' 
PRINT '=========================================================================================='

SELECT @cmd = '

	INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.tb_temp_proc2
	SELECT
		object_id,
		replace(definition,''DadosGx_ViaVR_AbrMai'',''' + LTRIM(RTRIM(@BancoDadosGX)) + ''')           -- REPLACE NO TEXTO QUE CONSTA NA PROCEDURE
	FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.tb_temp_proc1;
'

EXEC sp_executesql @cmd

PRINT '==========================================================================================' 
PRINT ' PREPARAÇÃO 05 - (up_Replace_Name_DadosGx_Procedures)' 
PRINT '=========================================================================================='

SELECT @cmd = '

	INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.tb_temp_proc3
	SELECT
		object_id,
		replace(definition,''CREATE PROCEDURE'',''ALTER PROCEDURE'')		-- ALTER PROCEDURE
	FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.tb_temp_proc2;
'

EXEC sp_executesql @cmd

PRINT '==========================================================================================' 
PRINT ' PREPARAÇÃO 06 - (up_Replace_Name_DadosGx_Procedures)' 
PRINT '=========================================================================================='

SELECT @CMD = '

	DECLARE @DEFINITION AS varchar(max);
	DECLARE @OBJECT_ID AS int;
	DECLARE CursorProc1 CURSOR FOR

		SELECT object_id, definition FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.tb_temp_proc3;

	OPEN CursorProc1;

		FETCH NEXT FROM CursorProc1  INTO @OBJECT_ID, @DEFINITION;

		WHILE @@FETCH_STATUS = 0
		BEGIN

			PRINT ''Compilando: ''+object_name(@OBJECT_ID)

			EXEC (@DEFINITION);

			FETCH NEXT FROM CursorProc1 INTO @OBJECT_ID, @DEFINITION;

		END;

	CLOSE CursorProc1;

	DEALLOCATE CursorProc1;

'

EXEC sp_executesql @CMD

PRINT '==========================================================================================' 
PRINT ' PREPARAÇÃO 07 -  (up_Replace_Name_DadosGx_Procedures)' 
PRINT '=========================================================================================='

SELECT @CMD = '

	IF EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects WHERE NAME = ''tb_temp_proc1'' AND TYPE = ''U'')
		BEGIN 
			DROP TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.tb_temp_proc1
		end 

	IF EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects WHERE NAME = ''tb_temp_proc2'' AND TYPE = ''U'')
		BEGIN 
			DROP TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.tb_temp_proc2
		end 

	IF EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects WHERE NAME = ''tb_temp_proc3'' AND TYPE = ''U'')
		BEGIN 
			DROP TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.tb_temp_proc3
		end 

'

EXEC sp_executesql @CMD

--	BEGIN
--	 PRINT 'QUERY EXECUTADA COM SUCESSO'
--	END
	
--GO 

--/* EXEC */
--EXEC dbo.up_Replace_Name_DadosGx_Procedures @BancoDadosGX

GO
-- <<< FIM: up_Replace_Name_DadosGx_Procedures.sql
GO


-- >>> INICIO: up_01_Extrai_Financeiro_gx.sql
-- =============================================================================
-- Layout: Financeiro
-- Staging : Arquivo_Financeiro_Tratado
-- Destino : Titulo_MG
-- Procedure: up_01_Extrai_Financeiro_gx (@BancoDadosGX, @BancoWF)
-- Depende : Pessoa_MG (CPF_CNPJ), Empresa_DePara (CNPJ_EMPRESA)
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Extrai_Financeiro_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_01_Extrai_Financeiro_gx;
GO

CREATE PROCEDURE dbo.up_01_Extrai_Financeiro_gx
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX)
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

SELECT @CMD = N'
    IF NOT EXISTS (
        SELECT 1 FROM ' + QUOTENAME(LTRIM(RTRIM(@BancoDadosGX))) + N'.sys.objects
        WHERE type = ''U'' AND name = ''Arquivo_Financeiro_Tratado''
    )
        SELECT @ok = 0
    ELSE
        SELECT @ok = 1
'
DECLARE @StagingOk BIT = 0
EXEC sp_executesql @CMD, N'@ok BIT OUTPUT', @ok = @StagingOk OUTPUT
IF @StagingOk = 0
BEGIN
    PRINT 'O < ARQUIVO > INFORMADO NAO EXISTE NESTE BANCO ' + @BancoDadosGX + '!'
    RETURN
END

PRINT '=========================================================================================='
PRINT ' Extrai_Titulo - Atualiza CPFCNPJ'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Arquivo_Financeiro_Tratado SET
        CPF_CNPJ = RTRIM(LTRIM(dbo.FN_RemoveCaracteresNaoInteiros(CPF_CNPJ)))
'
EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' Cria a cópia do Arquivo_Financeiro_Tratado para Migração'
PRINT '=========================================================================================='

SELECT @CMD = '
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects WHERE type = ''U'' AND name =''Titulo_MG'') )
    BEGIN
        SELECT a.* INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Titulo_MG
        FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Arquivo_Financeiro_Tratado a
        WHERE 1 = 1
    END
'
EXEC sp_executesql @CMD

SELECT @CMD = '
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''Empresa_Codigo'' AND obj.name = ''Titulo_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Titulo_MG ADD Empresa_Codigo int NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''Pessoa_DocIdentificador'' AND obj.name = ''Titulo_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Titulo_MG ADD Pessoa_DocIdentificador varchar(20) NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''Pessoa_Codigo'' AND obj.name = ''Titulo_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Titulo_MG ADD Pessoa_Codigo int NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''NaturezaOperacao_CodigoWF'' AND obj.name = ''Titulo_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Titulo_MG ADD NaturezaOperacao_CodigoWF int NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''TipoTitulo_CodigoWF'' AND obj.name = ''Titulo_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Titulo_MG ADD TipoTitulo_CodigoWF int NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''AgenteCobrador_CodigoWF'' AND obj.name = ''Titulo_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Titulo_MG ADD AgenteCobrador_CodigoWF int NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''ContaGerencial_CodigoWF'' AND obj.name = ''Titulo_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Titulo_MG ADD ContaGerencial_CodigoWF int NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''Departamento_CodigoWF'' AND obj.name = ''Titulo_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Titulo_MG ADD Departamento_CodigoWF int NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''TipoCobranca_CodigoWF'' AND obj.name = ''Titulo_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Titulo_MG ADD TipoCobranca_CodigoWF int NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''Banco_CodigoWF'' AND obj.name = ''Titulo_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Titulo_MG ADD Banco_CodigoWF int NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''Titulo_NSU'' AND obj.name = ''Titulo_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Titulo_MG ADD Titulo_NSU varchar(20) NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''TITULO_NUMERO_new'' AND obj.name = ''Titulo_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Titulo_MG ADD TITULO_NUMERO_new nvarchar(510) NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''Ocorrencia'' AND obj.name = ''Titulo_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Titulo_MG ADD Ocorrencia varchar(500) NULL
'
EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' AJUSTE de colunas VALOR com "","" na Tabela Titulo_MG'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.TITULO_SALDO = REPLACE(a.TITULO_SALDO,'','',''.''),
        a.TITULO_VALOR = REPLACE(a.TITULO_VALOR,'','',''.'')
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Titulo_MG a

    ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Titulo_MG ALTER COLUMN TITULO_SALDO FLOAT
    ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Titulo_MG ALTER COLUMN TITULO_VALOR FLOAT
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' Atualiza Flag=0 na Tabela Titulo_MG de EMPRESA não existe Empresa_DePara'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a SET a.Ocorrencia = '''' FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Titulo_MG a

    UPDATE a
    SET a.Ocorrencia = a.Ocorrencia + '' CNPJ_EMPRESA não existe Empresa_DePara. ''
        ,a.Flag = 0
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Titulo_MG a
    WHERE
        NOT EXISTS(
            SELECT 1
            FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Empresa_DePara b
            WHERE b.Pessoa_DocIdentificador = a.CNPJ_EMPRESA COLLATE DATABASE_DEFAULT
        )
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' Atualiza Flag=0 na Tabela Titulo_MG CPF/CNPJ em BRANCO'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.Ocorrencia = CASE
                        WHEN a.CPF_CNPJ IS NULL OR a.CPF_CNPJ = ''''
                        THEN a.Ocorrencia + '' | CPF CNPJ está vazio''
                        ELSE a.Ocorrencia
                    END,
        a.Flag = CASE
                    WHEN a.CPF_CNPJ IS NULL OR a.CPF_CNPJ = '''' THEN 0
                    ELSE a.Flag
                END
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Titulo_MG a
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' Atualiza Flag=0 na Tabela Titulo_MG TITULO_SALDO = 0'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.Flag = 0
        ,a.Ocorrencia = a.Ocorrencia + '' | TITULO_SALDO = 0.''
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Titulo_MG a
    WHERE
        a.Flag = 1 AND
        a.TITULO_SALDO = 0
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' AJUSTE de coluna TITULO_NUMERO_new'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.TITULO_NUMERO_new = CAST(TRY_CAST(a.TITULO_NUMERO AS bigint) AS nvarchar(510))
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Titulo_MG a
    WHERE
        a.Flag = 1 AND
        a.TITULO_NUMERO_new IS NULL
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' Atualiza Empresa na tabela Titulo_MG'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.Empresa_Codigo = b.Empresa_Codigo
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Titulo_MG a
    INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Empresa_DePara b on b.Pessoa_DocIdentificador = a.CNPJ_EMPRESA COLLATE DATABASE_DEFAULT
    WHERE
        a.Flag = 1
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' AJUSTE Pessoa_DocIdentificador na Tabela Titulo_MG'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.Pessoa_DocIdentificador = CASE
                                WHEN len(RTRIM(LTRIM(CPF_CNPJ))) < 11
                                    THEN replicate(''0'',11 - len(RTRIM(LTRIM(CPF_CNPJ)))) + RTRIM(LTRIM(CPF_CNPJ))
                                WHEN len(RTRIM(LTRIM(CPF_CNPJ))) > 11 and len(RTRIM(LTRIM(CPF_CNPJ))) < 14
                                    THEN replicate(''0'',14 - len(RTRIM(LTRIM(CPF_CNPJ)))) + RTRIM(LTRIM(CPF_CNPJ))
                                ELSE RTRIM(LTRIM(CPF_CNPJ))
                            END
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Titulo_MG a
    WHERE
        a.Flag = 1
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' Atualiza Pessoa na tabela Titulo_MG'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.Pessoa_Codigo = b.Pessoa_Codigo
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Titulo_MG a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Pessoa b on b.Pessoa_DocIdentificador = a.Pessoa_DocIdentificador COLLATE DATABASE_DEFAULT
    WHERE
        a.Flag = 1
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' Consulta Pessoa_DocIdentificador NÃO CADASTRADO'
PRINT '=========================================================================================='

SELECT @CMD = '
    IF(EXISTS(SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects WHERE name = ''Pessoa_Cadastrar'' AND type = ''U''))
    BEGIN
        DROP TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_Cadastrar
    END

    SELECT DISTINCT
        a.CPF_CNPJ
    INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_Cadastrar
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Titulo_MG a
    WHERE
        a.Flag = 1 AND
        a.Pessoa_Codigo IS NULL

    UPDATE a SET
        a.Ocorrencia = a.Ocorrencia + '' | Cadastrado de Pessoa não encontrado.''
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Titulo_MG a
    WHERE
        a.Flag = 1 AND
        a.Pessoa_Codigo IS NULL
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' Ajusta DATA_EMISSAO na Tabela Titulo_MG'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a SET
        a.DATA_EMISSAO = REPLACE(a.DATA_EMISSAO,''/'',''-'')
        ,a.DATA_ENTRADA = REPLACE(a.DATA_ENTRADA,''/'',''-'')
        ,a.DATA_VENCIMENTO = REPLACE(a.DATA_VENCIMENTO,''/'',''-'')
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Titulo_MG a
    WHERE
        a.Flag = 1
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' Atualiza NSU'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.Titulo_NSU = a.NSU
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Titulo_MG a
    WHERE
        a.Flag = 1
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' Atualiza Banco na tabela Titulo_MG'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.CODIGO_BANCO = dbo.FN_RemoveCaracteresNaoInteiros(CODIGO_BANCO)
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Titulo_MG a
    WHERE
        a.Flag = 1
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' VERIFICANDO CRITICAS DE EXTRAÇÃO DA TABELA Titulo_MG'
PRINT '=========================================================================================='

SELECT @CMD = '
    IF ( SELECT COUNT(*) FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Titulo_MG a WHERE a.Ocorrencia != '''' ) > 0
    BEGIN
        PRINT '' ENCONTROU CRITICAS DE EXTRAÇÃO DA TABELA <Titulo_MG>''

        SELECT
            ''Titulo_MG'' AS [TABELA],
            COUNT(*) AS [QTDE DE REGISTROS FLAG = 0 ]
        FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Titulo_MG a
        WHERE
            a.Flag = 0

        SELECT
            ''Titulo_MG'' AS [TABELA],
            COUNT(*) AS [QTDE DE REGISTROS COM OCORRÊNCIAS]
        FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Titulo_MG a
        WHERE
            a.Ocorrencia != ''''
    END
    ELSE
        PRINT '' NÃO ENCONTROU CRITICAS DE EXTRAÇÃO DA TABELA <Titulo_MG>''
'
EXEC sp_executesql @CMD
GO
-- <<< FIM: up_01_Extrai_Financeiro_gx.sql
GO


-- >>> INICIO: up_01_Extrai_Pessoa_gx.sql
-- =============================================================================
-- Layout: 1 Forn_cli.txt
-- Staging : Arquivo_Forn_cli_Tratado
-- Destino : Pessoa_MG
-- Procedure: up_01_Extrai_Pessoa_gx (@BancoDadosGX, @BancoWF)
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================
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
-- <<< FIM: up_01_Extrai_Pessoa_gx.sql
GO


-- >>> INICIO: up_07_Extrai_PessoaConjuge_gx.sql
-- =============================================================================
-- Layout: Forn_cli_Conjuge.txt
-- Staging : Arquivo_Forn_cli_Conjuge_Tratado
-- Destino : PessoaConjuge_MG
-- Procedure: up_07_Extrai_PessoaConjuge_gx (@BancoDadosGX)
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================
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
		a.DT_ANIVER_CONJUGE = CASE
						WHEN ISDATE(REPLACE(a.DT_ANIVER_CONJUGE,''/'',''-'')) = 0 THEN ''1900-01-01'' 
						ELSE REPLACE(a.DT_ANIVER_CONJUGE,''/'',''-'')  
					END
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
-- <<< FIM: up_07_Extrai_PessoaConjuge_gx.sql
GO


-- >>> INICIO: up_06_Extrai_PessoaContato_gx.sql
-- =============================================================================
-- Layout: Forn_cli_Contato.txt
-- Staging : Arquivo_Forn_cli_Contato_Tratado
-- Destino : PessoaContato_MG
-- Procedure: up_06_Extrai_PessoaContato_gx (@BancoDadosGX)
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================
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
		a.DT_ANIVER = CASE
						WHEN ISDATE(REPLACE(a.DT_ANIVER,''/'',''-'')) = 0 THEN ''1900-01-01'' 
						ELSE REPLACE(a.DT_ANIVER,''/'',''-'')  
					END
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
-- <<< FIM: up_06_Extrai_PessoaContato_gx.sql
GO


-- >>> INICIO: up_08_Extrai_PessoaBanco_gx.sql
-- =============================================================================
-- Layout: Forn_cli_DadosBancarios.txt
-- Staging : Arquivo_Forn_cli_DadosBancarios_Tratado
-- Destino : PessoaBanco_MG
-- Procedure: up_08_Extrai_PessoaBanco_gx (@BancoDadosGX)
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_08_Extrai_PessoaBanco_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_08_Extrai_PessoaBanco_gx;
GO

CREATE PROCEDURE dbo.up_08_Extrai_PessoaBanco_gx
	@BancoDadosGX		VARCHAR(MAX)
	
AS

DECLARE @CMD NVARCHAR(MAX)
-- ==========================================================================

	IF ( NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX) )
		BEGIN 
			PRINT 'O < BANCO DE DADOSGX > INFORMADO NAO EXISTE NESTE SERVIDOR!'
			RETURN
		END

	IF ( NOT EXISTS (SELECT 1 FROM SYS.OBJECTS WHERE NAME = 'Arquivo_Forn_cli_DadosBancarios_Tratado') )
		BEGIN 
			PRINT 'O < ARQUIVO > INFORMADO NAO EXISTE NESTE BANCO '+ @BancoDadosGX +'!'
			RETURN
		END

-- ==========================================================================================

--DROP TABLE dbo.PessoaBanco_MG

PRINT '==========================================================================================' 
PRINT ' PessoaBanco - Atualiza CPF_CNPJ ' 
PRINT '=========================================================================================='

SELECT @CMD = '

	UPDATE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Arquivo_Forn_cli_DadosBancarios_Tratado SET
		CPF_CNPJ = rtrim(ltrim(dbo.FN_RemoveCaracteresNaoInteiros(CPF_CNPJ)))

'

EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' Cria a cópia do Arquivo_Forn_cli_DadosBancarios_Tratado para Migração' 
PRINT '=========================================================================================='

SELECT @CMD = '

	IF(NOT EXISTS(SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects WHERE type = ''U'' AND name =''PessoaBanco_MG''))
	BEGIN
		SELECT A.*,1 AS Flag into ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaBanco_MG FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Arquivo_Forn_cli_DadosBancarios_Tratado a

		ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaBanco_MG	ADD Pessoa_DocIdentificador varchar (20)	NULL
		ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaBanco_MG	ADD	Ocorrencia				VARCHAR(500)

	END

'
EXEC sp_executesql @CMD

PRINT '==========================================================================================' 
PRINT ' Atualiza Flag = 0 na PessoaBanco_MG CPF/CNPJ em BRANCO ' 
PRINT '=========================================================================================='

SELECT @CMD = '

	UPDATE a SET a.Ocorrencia = '''' FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaBanco_MG a

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
	FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaBanco_MG a		

'

EXEC sp_executesql @CMD

PRINT '==========================================================================================' 
PRINT ' Atualiza Flag = 0 Pessoa já cadastrado em WF-Produção ' 
PRINT '=========================================================================================='

SELECT @CMD = '

	UPDATE a SET
		a.Flag = 0
		,a.Ocorrencia = a.Ocorrencia + '' Cadastrado da Pessoa no WF-Produção.''
	FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaBanco_MG a
	INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_EmProducao b ON a.CPF_CNPJ = b.CPF_CNPJ

'

EXEC sp_executesql @CMD

PRINT '==========================================================================================' 
PRINT ' Atualiza Flag na Tabela PessoaBanco_MG CPF/CNPJ não existe na Pessoa_MG' 
PRINT '=========================================================================================='

SELECT @CMD = '

	UPDATE a SET
		a.Flag = 0
		,a.Ocorrencia = a.Ocorrencia + '' | CPF CNPJ não existe na Pessoa_MG.''
	FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaBanco_MG a	
	WHERE 
		1=1 AND
		NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_MG b WHERE a.CPF_CNPJ = b.CPF_CNPJ)

'

EXEC sp_executesql @CMD

PRINT '==========================================================================================' 
PRINT ' Ajusta Pessoa_DocIdentificador na Tabela PessoaBanco_MG ' 
PRINT '=========================================================================================='

SELECT @CMD = '

	UPDATE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaBanco_MG SET
	Pessoa_DocIdentificador = CASE 
								WHEN len(RTRIM(LTRIM(CPF_CNPJ))) < 11 
									THEN replicate(''0'',11 - len(RTRIM(LTRIM(CPF_CNPJ)))) + RTRIM(LTRIM(CPF_CNPJ)) 
								WHEN len(RTRIM(LTRIM(CPF_CNPJ))) > 11 AND len(RTRIM(LTRIM(CPF_CNPJ))) < 14 
									THEN replicate(''0'',14 - len(RTRIM(LTRIM(CPF_CNPJ)))) + RTRIM(LTRIM(CPF_CNPJ)) 
								ELSE RTRIM(LTRIM(CPF_CNPJ)) 
							END
	WHERE
		Flag = 1
'

EXEC sp_executesql @CMD

GO
-- <<< FIM: up_08_Extrai_PessoaBanco_gx.sql
GO


-- >>> INICIO: up_03_Extrai_PessoaDoc_gx.sql
-- =============================================================================
-- Layout: 2 Forn_cli_Documento.txt
-- Staging : Arquivo_Forn_Cli_Documento_Tratado
-- Destino : PessoaDocumento_MG
-- Procedure: up_03_Extrai_PessoaDoc_gx (@BancoDadosGX)
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_03_Extrai_PessoaDoc_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_03_Extrai_PessoaDoc_gx;
GO

CREATE PROCEDURE dbo.up_03_Extrai_PessoaDoc_gx
    @BancoDadosGX VARCHAR(MAX)
AS

DECLARE @CMD NVARCHAR(MAX)

    IF ( NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX) )
        BEGIN
            PRINT 'O < '+ @BancoDadosGX +' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
            RETURN
        END
    SELECT @CMD = N'
        IF NOT EXISTS (
            SELECT 1 FROM ' + QUOTENAME(LTRIM(RTRIM(@BancoDadosGX))) + N'.sys.objects
            WHERE type = ''U'' AND name = ''Arquivo_Forn_Cli_Documento_Tratado''
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

PRINT '=========================================================================================='
PRINT ' Extrai PessoaDocumento - Atualiza CPF_CNPJ '
PRINT '=========================================================================================='

SELECT @CMD = '

    UPDATE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Arquivo_Forn_Cli_Documento_Tratado SET
        CPF_CNPJ = RTRIM(LTRIM(' + QUOTENAME(LTRIM(RTRIM(@BancoDadosGX))) + '.dbo.fn_RemoveCaracteresNaoInteiros(CPF_CNPJ)))

'

EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' Cria a cópia do Arquivo_Forn_Cli_Documento_Tratado para Migração'
PRINT '=========================================================================================='

SELECT @CMD = '

    IF(NOT EXISTS(SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects WHERE type = ''U'' AND name =''PessoaDocumento_MG''))
    BEGIN
        SELECT a.* INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaDocumento_MG FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Arquivo_Forn_Cli_Documento_Tratado a WHERE 1=1
    END

'

EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' Adicionando colunas na tabela PessoaDocumento_MG '
PRINT '=========================================================================================='

SELECT @CMD = '

    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col, ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj
                    WHERE col.object_id = obj.object_id AND col.name = ''Pessoa_DocIdentificador'' AND obj.name = ''PessoaDocumento_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaDocumento_MG ADD Pessoa_DocIdentificador varchar(20) NULL

    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col, ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj
                    WHERE col.object_id = obj.object_id AND col.name = ''Ocorrencia'' AND obj.name = ''PessoaDocumento_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaDocumento_MG ADD Ocorrencia VARCHAR(500)

'

EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' Atualiza Flag = 0 na PessoaDocumento_MG CPF/CNPJ em BRANCO '
PRINT '=========================================================================================='

SELECT @CMD = '

    UPDATE a SET a.Ocorrencia = '''' FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaDocumento_MG a

    UPDATE a
    SET    a.Ocorrencia = CASE
                        WHEN CPF_CNPJ IS NULL OR CPF_CNPJ = ''''
                        THEN a.Ocorrencia + '' | CPF CNPJ está vazio''
                        ELSE a.Ocorrencia
                    END,
        a.Flag = CASE
                    WHEN CPF_CNPJ IS NULL OR CPF_CNPJ = '''' THEN 0
                    ELSE 1
                END
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaDocumento_MG a

'

EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' Atualiza Flag = 0 Pessoa já cadastrado em WF-Produção '
PRINT '=========================================================================================='

SELECT @CMD = '

    UPDATE a SET
        a.Flag = 0
        ,a.Ocorrencia = a.Ocorrencia + '' Cadastrado da Pessoa no WF-Produção.''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaDocumento_MG a
    INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_EmProducao b ON a.CPF_CNPJ = b.CPF_CNPJ

'

EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' Atualiza Flag=0 na Tabela PessoaDocumento_MG CPF/CNPJ não existe na Pessoa_MG'
PRINT '=========================================================================================='

SELECT @CMD = '

    UPDATE a SET
        a.Flag = 0
        ,a.Ocorrencia = a.Ocorrencia + '' | CPF CNPJ não existe na Pessoa_MG.''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaDocumento_MG a
    WHERE
        1=1 AND
        NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_MG b WHERE a.CPF_CNPJ = b.CPF_CNPJ)

'

EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' Ajusta Pessoa_DocIdentificador na Tabela PessoaDocumento_MG '
PRINT '=========================================================================================='

SELECT @CMD = '

    UPDATE a
    SET    a.Pessoa_DocIdentificador = CASE
                                WHEN len(RTRIM(LTRIM(CPF_CNPJ))) < 11
                                    THEN replicate(''0'',11 - len(RTRIM(LTRIM(CPF_CNPJ)))) + RTRIM(LTRIM(CPF_CNPJ))
                                WHEN len(RTRIM(LTRIM(CPF_CNPJ))) > 11 and len(RTRIM(LTRIM(CPF_CNPJ))) < 14
                                    THEN replicate(''0'',14 - len(RTRIM(LTRIM(CPF_CNPJ)))) + RTRIM(LTRIM(CPF_CNPJ))
                                ELSE RTRIM(LTRIM(CPF_CNPJ))
                            END
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaDocumento_MG a
    WHERE
        a.Flag = 1
'

EXEC sp_executesql @CMD

GO
-- <<< FIM: up_03_Extrai_PessoaDoc_gx.sql
GO


-- >>> INICIO: up_02_Extrai_PessoaEndereco_gx.sql
-- =============================================================================
-- Layout: Forn_cli_Endereco.txt
-- Staging : Arquivo_Forn_Cli_Endereco_Tratado
-- Destino : PessoaEndereco_MG
-- Procedure: up_02_Extrai_PessoaEndereco_gx (@BancoDadosGX)
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================
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
	IF ( NOT EXISTS (SELECT 1 FROM SYS.OBJECTS WHERE NAME = 'Arquivo_Forn_Cli_Endereco_Tratado') )
		BEGIN 
			PRINT 'O < ARQUIVO > INFORMADO NAO EXISTE NESTE BANCO '+ @BancoDadosGX +'!'
			RETURN
		END
-- ==========================================================================================

--DROP TABLE dbo.PessoaEndereco_MG

PRINT '==========================================================================================' 
PRINT ' Extrai PessoaEndereco - Atualiza CPFCNPJ ' 
PRINT '=========================================================================================='

SELECT @CMD = '

	UPDATE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Arquivo_Forn_Cli_Endereco_Tratado SET
		CPF_CNPJ = rtrim(ltrim(dbo.FN_RemoveCaracteresNaoInteiros(CPF_CNPJ)))

'

EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' Cria a cópia do Arquivo_Forn_Cli_Endereco_tratado para Migração' 
PRINT '=========================================================================================='

SELECT @CMD = '

	IF(NOT EXISTS(SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects WHERE type = ''U'' AND name =''PessoaEndereco_MG''))
	BEGIN
		SELECT A.*,1 as Flag INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaEndereco_MG FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Arquivo_Forn_Cli_Endereco_Tratado a
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
-- <<< FIM: up_02_Extrai_PessoaEndereco_gx.sql
GO


-- >>> INICIO: up_04_Extrai_PessoaEnquadramento_gx.sql
-- =============================================================================
-- Layout: Forn_cli_Enquadramento.txt
-- Staging : Arquivo_Forn_Cli_Enquadramento_Tratado
-- Destino : PessoaEnquadramento_MG
-- Procedure: up_04_Extrai_PessoaEnquadramento_gx (@BancoDadosGX)
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================
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
		a.DATA_CADASTRO = cast(b.DATA_CADASTRO as date) 
	FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaEnquadramento_MG a	
	INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_MG b ON a.CPF_CNPJ = b.CPF_CNPJ
	WHERE
		a.Flag = 1

'

EXEC sp_executesql @CMD

GO
-- <<< FIM: up_04_Extrai_PessoaEnquadramento_gx.sql
GO


-- >>> INICIO: up_05_Extrai_PessoaTelefone_gx.sql
-- =============================================================================
-- Layout: Forn_cli_Telefone.txt
-- Staging : Arquivo_Forn_Cli_Telefone_Tratado
-- Destino : PessoaTelefone_MG
-- Procedure: up_05_Extrai_PessoaTelefone_gx (@BancoDadosGX, @DDDPadrao char(2))
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================
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
-- <<< FIM: up_05_Extrai_PessoaTelefone_gx.sql
GO


-- >>> INICIO: up_01_Extrai_Fseg_Cab_gx.sql
-- =============================================================================
-- Layout: 13 Fseg_Cab
-- Staging : Arquivo_FSeg_Cab_Tratado
-- Destino : Ficha_Cab_MG
-- Procedure: up_01_Extrai_Fseg_Cab_gx (@BancoDadosGX, @BancoWF)
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Extrai_Fseg_Cab_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_01_Extrai_Fseg_Cab_gx;
GO

CREATE PROCEDURE dbo.up_01_Extrai_Fseg_Cab_gx
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX)
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

SELECT @CMD = N'
    IF NOT EXISTS (
        SELECT 1 FROM ' + QUOTENAME(LTRIM(RTRIM(@BancoDadosGX))) + N'.sys.objects
        WHERE type = ''U'' AND name = ''Arquivo_FSeg_Cab_Tratado''
    )
        SELECT @ok = 0
    ELSE
        SELECT @ok = 1
'
DECLARE @StagingOk BIT = 0
EXEC sp_executesql @CMD, N'@ok BIT OUTPUT', @ok = @StagingOk OUTPUT
IF @StagingOk = 0
BEGIN
    PRINT 'O < ARQUIVO > INFORMADO NAO EXISTE NESTE BANCO ' + @BancoDadosGX + '!'
    RETURN
END

SELECT @CMD = '
    UPDATE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Arquivo_FSeg_Cab_Tratado SET
        CPF_CNPJ = RTRIM(LTRIM(dbo.FN_RemoveCaracteresNaoInteiros(CPF_CNPJ)))
'
EXEC sp_executesql @CMD

SELECT @CMD = '
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects WHERE type = ''U'' AND name = ''Ficha_Cab_MG'') )
    BEGIN
        SELECT a.* INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG
        FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Arquivo_FSeg_Cab_Tratado a
        WHERE 1 = 1
    END
'
EXEC sp_executesql @CMD

SELECT @CMD = '
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''Veiculo_Codigo'' AND obj.name = ''Ficha_Cab_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG ADD Veiculo_Codigo int NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''Empresa_Codigo'' AND obj.name = ''Ficha_Cab_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG ADD Empresa_Codigo int NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''Pessoa_DocIdentificador'' AND obj.name = ''Ficha_Cab_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG ADD Pessoa_DocIdentificador varchar(20) NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''Pessoa_Codigo'' AND obj.name = ''Ficha_Cab_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG ADD Pessoa_Codigo int NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''TipoOSCod'' AND obj.name = ''Ficha_Cab_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG ADD TipoOSCod int NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''Maquina_Implemento'' AND obj.name = ''Ficha_Cab_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG ADD Maquina_Implemento int NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''Ocorrencia'' AND obj.name = ''Ficha_Cab_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG ADD Ocorrencia varchar(500) NULL
'
EXEC sp_executesql @CMD

SELECT @CMD = '
    UPDATE a SET a.Ocorrencia = '''' FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG a

    UPDATE a
    SET a.Ocorrencia = CASE WHEN CHASSI IS NULL OR CHASSI = '''' THEN a.Ocorrencia + ''CHASSI é BRANCO/NULO.'' ELSE a.Ocorrencia END,
        a.Flag = CASE WHEN CHASSI IS NULL OR CHASSI = '''' THEN 0 ELSE ISNULL(a.Flag, 1) END
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG a

    UPDATE a
    SET a.Ocorrencia = CASE WHEN CPF_CNPJ IS NULL OR CPF_CNPJ = '''' THEN a.Ocorrencia + '' | CPF_CNPJ é BRANCO/NULO.'' ELSE a.Ocorrencia END,
        a.Flag = CASE WHEN CPF_CNPJ IS NULL OR CPF_CNPJ = '''' THEN 0 ELSE a.Flag END
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG a

    UPDATE a
    SET a.Ocorrencia = CASE WHEN NUMERO_OS IS NULL OR NUMERO_OS = '''' THEN a.Ocorrencia + '' | NUMERO_OS é BRANCO/NULO.'' ELSE a.Ocorrencia END,
        a.Flag = CASE WHEN NUMERO_OS IS NULL OR NUMERO_OS = '''' THEN 0 ELSE a.Flag END
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG a
'
EXEC sp_executesql @CMD

SELECT @CMD = '
    IF(EXISTS(SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects WHERE name = ''Duplicidade_Ficha_Cab'' AND type = ''U''))
        DROP TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Duplicidade_Ficha_Cab

    SELECT a.CHASSI, a.NUMERO_OS, COUNT(a.CHASSI) AS QUANTIDADE, MAX(a.IDTabela) AS ID
    INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Duplicidade_Ficha_Cab
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG a
    GROUP BY a.CHASSI, a.NUMERO_OS, a.TIPO_OS_CODIGO, a.DATA_ABERTURA, a.DATA_LIBERACAO, a.CNPJ_EMPRESA
    HAVING COUNT(a.CHASSI) > 1

    UPDATE a
    SET a.Flag = 0,
        a.Ocorrencia = a.Ocorrencia + '' | Registros Duplicados.''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG a
    INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Duplicidade_Ficha_Cab b
        ON a.CHASSI = b.CHASSI COLLATE DATABASE_DEFAULT
       AND a.NUMERO_OS = b.NUMERO_OS COLLATE DATABASE_DEFAULT
    WHERE a.IDtabela < b.ID

    UPDATE a
    SET a.CHASSI = UPPER(dbo.fn_Remove_Caracteres_Especiais(a.CHASSI))
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG a
    WHERE a.Flag = 1

    UPDATE a
    SET a.Ocorrencia = a.Ocorrencia + '' | QTDE de Caracteres no CHASSI é INVÁLIDA.''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG a
    WHERE a.Flag = 1 AND (LEN(a.CHASSI) > 17 OR LEN(a.CHASSI) < 17)

    UPDATE a
    SET a.Pessoa_DocIdentificador = CASE
            WHEN LEN(RTRIM(LTRIM(CPF_CNPJ))) < 11
                THEN REPLICATE(''0'', 11 - LEN(RTRIM(LTRIM(CPF_CNPJ)))) + RTRIM(LTRIM(CPF_CNPJ))
            WHEN LEN(RTRIM(LTRIM(CPF_CNPJ))) > 11 AND LEN(RTRIM(LTRIM(CPF_CNPJ))) < 14
                THEN REPLICATE(''0'', 14 - LEN(RTRIM(LTRIM(CPF_CNPJ)))) + RTRIM(LTRIM(CPF_CNPJ))
            ELSE RTRIM(LTRIM(CPF_CNPJ))
        END
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG a
    WHERE a.Flag = 1
'
EXEC sp_executesql @CMD

SELECT @CMD = '
    IF (SELECT COUNT(*) FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG WHERE ISNUMERIC(KM) = 0) > 0
    BEGIN
        UPDATE a
        SET a.KM = CASE
                WHEN dbo.FN_RemoveCaracteresNaoInteiros(a.KM) IS NULL OR dbo.FN_RemoveCaracteresNaoInteiros(a.KM) = 0 THEN 1
                ELSE dbo.FN_RemoveCaracteresNaoInteiros(a.KM)
            END
        FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG a
        WHERE a.Flag = 1
    END

    IF (SELECT COUNT(*) FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG WHERE ISNUMERIC(NUMERO_OS) = 0) > 0
    BEGIN
        UPDATE a
        SET a.NUMERO_OS = CASE
                WHEN dbo.FN_RemoveCaracteresNaoInteiros(a.NUMERO_OS) IS NULL OR dbo.FN_RemoveCaracteresNaoInteiros(a.NUMERO_OS) = 0 THEN 1
                ELSE dbo.FN_RemoveCaracteresNaoInteiros(a.NUMERO_OS)
            END
        FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG a
        WHERE a.Flag = 1
    END

    UPDATE a
    SET a.DATA_ABERTURA = CASE
            WHEN a.DATA_ABERTURA IS NULL OR REPLACE(a.DATA_ABERTURA, ''/'', ''-'') = '''' OR ISDATE(a.DATA_ABERTURA) = 0 THEN ''1900-01-01''
            ELSE REPLACE(a.DATA_ABERTURA, ''/'', ''-'')
        END
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG a
    WHERE a.Flag = 1

    UPDATE a
    SET a.DATA_LIBERACAO = CASE
            WHEN a.DATA_LIBERACAO IS NULL OR REPLACE(a.DATA_LIBERACAO, ''/'', ''-'') = '''' OR ISDATE(a.DATA_LIBERACAO) = 0 THEN ''1900-01-01''
            ELSE REPLACE(a.DATA_LIBERACAO, ''/'', ''-'')
        END
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG a
    WHERE a.Flag = 1

    UPDATE a SET a.Veiculo_Codigo = b.Veiculo_Codigo
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Veiculo b
        ON a.CHASSI = b.Veiculo_Chassi COLLATE DATABASE_DEFAULT
    WHERE a.Flag = 1

    UPDATE a SET a.Ocorrencia = a.Ocorrencia + '' | CADASTRAR VEÍCULO no WF.''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG a
    WHERE a.Flag = 1 AND a.Veiculo_Codigo IS NULL

    UPDATE a SET a.Pessoa_Codigo = b.Pessoa_Codigo
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Pessoa b
        ON a.Pessoa_DocIdentificador = b.Pessoa_DocIdentificador COLLATE DATABASE_DEFAULT
    WHERE a.Flag = 1

    UPDATE a SET a.Empresa_Codigo = b.Empresa_Codigo
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG a
    INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Empresa_DePara b
        ON b.Pessoa_DocIdentificador = a.CNPJ_EMPRESA
    WHERE a.Flag = 1

    UPDATE a SET a.Ocorrencia = a.Ocorrencia + '' | EMPRESA não está na Empresa_DePara.''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG a
    WHERE a.Flag = 1 AND a.Empresa_Codigo IS NULL
'
EXEC sp_executesql @CMD
GO
-- <<< FIM: up_01_Extrai_Fseg_Cab_gx.sql
GO


-- >>> INICIO: up_01_Extrai_Fseg_Prd_gx.sql
-- =============================================================================
-- Layout: 14 Fseg_Prd
-- Staging : Arquivo_FSeg_Prd_Tratado
-- Destino : Ficha_Prd_MG
-- Procedure: up_01_Extrai_Fseg_Prd_gx (@BancoDadosGX, @BancoWF)
-- Depende : Ficha_Cab_MG (layout 13 Fseg_Cab)
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Extrai_Fseg_Prd_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_01_Extrai_Fseg_Prd_gx;
GO

CREATE PROCEDURE dbo.up_01_Extrai_Fseg_Prd_gx
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX)
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

SELECT @CMD = N'
    IF NOT EXISTS (
        SELECT 1 FROM ' + QUOTENAME(LTRIM(RTRIM(@BancoDadosGX))) + N'.sys.objects
        WHERE type = ''U'' AND name = ''Arquivo_FSeg_Prd_Tratado''
    )
        SELECT @ok = 0
    ELSE
        SELECT @ok = 1
'
DECLARE @StagingOk BIT = 0
EXEC sp_executesql @CMD, N'@ok BIT OUTPUT', @ok = @StagingOk OUTPUT
IF @StagingOk = 0
BEGIN
    PRINT 'O < ARQUIVO > INFORMADO NAO EXISTE NESTE BANCO ' + @BancoDadosGX + '!'
    RETURN
END

PRINT '=========================================================================================='
PRINT ' Cria a cópia do Arquivo_FSeg_Prd_Tratado para Migração'
PRINT '=========================================================================================='

SELECT @CMD = '
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects WHERE type = ''U'' AND name =''Ficha_Prd_MG'') )
    BEGIN
        SELECT a.* INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG
        FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Arquivo_FSeg_Prd_Tratado a
        WHERE 1 = 1
    END
'
EXEC sp_executesql @CMD

SELECT @CMD = '
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''Veiculo_Codigo'' AND obj.name = ''Ficha_Prd_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG ADD Veiculo_Codigo int NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''Produto_Codigo'' AND obj.name = ''Ficha_Prd_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG ADD Produto_Codigo int NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''ProdutoMarca_MarcaCod'' AND obj.name = ''Ficha_Prd_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG ADD ProdutoMarca_MarcaCod int NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''PRODUTO_REFERENCIA_Ajustado'' AND obj.name = ''Ficha_Prd_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG ADD PRODUTO_REFERENCIA_Ajustado nvarchar(510) NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''PRODUTO_REFERENCIATRANS'' AND obj.name = ''Ficha_Prd_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG ADD PRODUTO_REFERENCIATRANS nvarchar(510) NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''Empresa_Codigo'' AND obj.name = ''Ficha_Prd_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG ADD Empresa_Codigo int NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''TipoOSCod'' AND obj.name = ''Ficha_Prd_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG ADD TipoOSCod int NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''Ocorrencia'' AND obj.name = ''Ficha_Prd_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG ADD Ocorrencia varchar(500) NULL
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' Atualiza Flag=0 na Tabela Ficha_Prd_MG CHASSI em BRANCO/NULO'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a SET a.Ocorrencia = '''' FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG a

    UPDATE a
    SET a.Ocorrencia = CASE
                        WHEN CHASSI IS NULL OR CHASSI = ''''
                        THEN a.Ocorrencia + ''CHASSI é BRANCO/NULO.''
                        ELSE a.Ocorrencia
                    END,
        a.Flag = CASE
                    WHEN CHASSI IS NULL OR CHASSI = '''' THEN 0
                    ELSE a.Flag
                END
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG a
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' Atualiza Flag=0 na Tabela Ficha_Prd_MG PRODUTO_REFERENCIA em BRANCO/NULO'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.Ocorrencia = CASE
                        WHEN PRODUTO_REFERENCIA IS NULL OR PRODUTO_REFERENCIA = ''''
                        THEN a.Ocorrencia + '' | PRODUTO_REFERENCIA é BRANCO/NULO.''
                        ELSE a.Ocorrencia
                    END,
        a.Flag = CASE
                    WHEN PRODUTO_REFERENCIA IS NULL OR PRODUTO_REFERENCIA = '''' THEN 0
                    ELSE a.Flag
                END
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG a
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' Atualiza Flag=0 na Tabela Ficha_Prd_MG NUMERO_OS em BRANCO/NULO'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.Ocorrencia = CASE
                        WHEN NUMERO_OS IS NULL OR NUMERO_OS = ''''
                        THEN a.Ocorrencia + '' | NUMERO_OS é BRANCO/NULO.''
                        ELSE a.Ocorrencia
                    END,
        a.Flag = CASE
                    WHEN NUMERO_OS IS NULL OR NUMERO_OS = '''' THEN 0
                    ELSE a.Flag
                END
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG a
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' Atualiza Flag=0 na Tabela Ficha_Prd_MG Chassi NÃO EXISTE na Ficha_Cab_MG'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.Flag = 0
        ,a.Ocorrencia = a.Ocorrencia + '' | Chassi NÃO EXISTE na Ficha_Cab_MG.''
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG a
    WHERE
        NOT EXISTS(
            SELECT 1
            FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG b
            WHERE
                a.CHASSI = b.CHASSI
        )
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' Atualiza Flag=0 na Tabela Ficha_Prd_MG PRODUTO_QUANTIDADE = 0'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.Flag = 0
        ,a.Ocorrencia = a.Ocorrencia + '' | PRODUTO_QUANTIDADE = 0.''
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG a
    WHERE
        a.Flag = 1 AND
        (PRODUTO_QUANTIDADE IS NULL OR PRODUTO_QUANTIDADE = ''''
        OR TRY_CAST(RTRIM(LTRIM(PRODUTO_QUANTIDADE)) AS DECIMAL(18, 6)) = 0)
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' Atualiza Flag=0 na Tabela Ficha_Prd_MG devido o Flag = 0 na Ficha_Cab_MG'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.Flag = 0
        ,a.Ocorrencia = a.Ocorrencia + '' | Registro é FLAG=0 na Ficha_Cab_MG.''
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG a
    WHERE
        EXISTS(
            SELECT 1
            FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG b
            WHERE
                a.CHASSI = b.CHASSI AND
                a.NUMERO_OS = b.NUMERO_OS AND
                b.Flag = 0
        )
'
EXEC sp_executesql @CMD

/* VERIFICAR COM MAIS CALMA — duplicidade comentada no legado
PRINT '=========================================================================================='
PRINT ' Remove as duplicidades da Ficha_Prd_MG'
PRINT '=========================================================================================='
*/


PRINT '=========================================================================================='
PRINT ' Ajusta campo CHASSI na Tabela Ficha_Prd_MG'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET
        a.CHASSI = UPPER(dbo.fn_Remove_Caracteres_Especiais(a.CHASSI))
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG a
    WHERE
        a.Flag = 1
'
EXEC sp_executesql @CMD


PRINT '========================================================================================='
PRINT ' Atualiza Ocorrencia na Ficha_Prd_MG para LEN(CHASSI)>17 OR LEN(CHASSI)<17'
PRINT '========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.Ocorrencia = a.Ocorrencia + '' | QTDE de Caracteres no CHASSI é INVÁLIDA.''
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG a
    WHERE
        a.Flag = 1 AND
        (LEN(a.CHASSI) > 17 OR LEN(CHASSI) < 17)
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' Ajusta campo NUMERO_OS na Tabela Ficha_Prd_MG'
PRINT '=========================================================================================='

SELECT @CMD = '
    IF( SELECT COUNT(*) FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG WHERE ISNUMERIC(NUMERO_OS) = 0 ) > 0
    BEGIN
        UPDATE a
        SET a.NUMERO_OS = CASE
                            WHEN dbo.FN_RemoveCaracteresNaoInteiros(a.NUMERO_OS) IS NULL OR dbo.FN_RemoveCaracteresNaoInteiros(a.NUMERO_OS) = 0 THEN 1
                            ELSE dbo.FN_RemoveCaracteresNaoInteiros(a.NUMERO_OS)
                        END
        FROM
            ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG a
        WHERE
            a.Flag = 1
    END

    ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG ALTER COLUMN NUMERO_OS int;
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' Atualiza Veiculo_Codigo na tabela Ficha_Prd_MG'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a SET
        a.Veiculo_Codigo = b.Veiculo_Codigo
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Veiculo b ON a.CHASSI = b.Veiculo_Chassi COLLATE DATABASE_DEFAULT
    WHERE
        a.Flag = 1
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' Atualiza Marca e Empresa na tabela Ficha_Prd_MG'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.ProdutoMarca_MarcaCod = b.Empresa_MarcaCod
        ,a.Empresa_Codigo = b.Empresa_Codigo
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG a
    INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Empresa_DePara b on b.Pessoa_DocIdentificador = a.CNPJ_EMPRESA collate database_default
    WHERE
        a.Flag = 1
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' Atualiza Flag=0 na Tabela Ficha_Prd_MG Empresa_Codigo em VAZIO/NULO'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.Flag = 0
        ,a.Ocorrencia = a.Ocorrencia + '' | Empresa_Codigo é VAZIO/NULO.''
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG a
    WHERE
        a.Flag = 1 AND
        a.Empresa_Codigo IS NULL
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' Atualiza PRODUTO_REFERENCIA / PRODUTO_REFERENCIATRANS na tabela Ficha_Prd_MG'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.PRODUTO_REFERENCIA_Ajustado = RTRIM(LTRIM(a.PRODUTO_REFERENCIA))
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG a
    WHERE
        a.Flag = 1 AND
        a.ProdutoMarca_MarcaCod NOT IN (14,36,54) --FORD / VOLKS / MAN

    UPDATE a
    SET a.PRODUTO_REFERENCIATRANS = (dbo.fn_Remove_Caracteres_Especiais(a.PRODUTO_REFERENCIA_Ajustado))
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG a
    WHERE
        a.Flag = 1 AND
        a.ProdutoMarca_MarcaCod NOT IN (14,36,54) --FORD / VOLKS / MAN
'
EXEC sp_executesql @CMD


PRINT '============================================================================================================'
PRINT ' Atualiza PRODUTO_REFERENCIA / PRODUTO_REFERENCIATRANS das marcas FORD / VOLKS / MAN na Tabela Ficha_Prd_MG'
PRINT '============================================================================================================'

SELECT @CMD = '
    IF EXISTS ( SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG WHERE ProdutoMarca_MarcaCod IN (14,36,54) )
    BEGIN
        UPDATE a
        SET a.PRODUTO_REFERENCIA_Ajustado = CASE
                                            WHEN LEN(REPLICATE('' '',5 - LEN(SUBSTRING(a.PRODUTO_REFERENCIA,1,CHARINDEX(''/'',a.PRODUTO_REFERENCIA)))) + a.PRODUTO_REFERENCIA) > 30 THEN a.PRODUTO_REFERENCIA
                                            ELSE REPLICATE('' '',5 - LEN(SUBSTRING(a.PRODUTO_REFERENCIA,1,CHARINDEX(''/'',a.PRODUTO_REFERENCIA)))) + a.PRODUTO_REFERENCIA
                                            END
        FROM
            ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG a
        WHERE
            a.Flag = 1 AND
            a.ProdutoMarca_MarcaCod IN (14,36,54) AND
            a.PRODUTO_REFERENCIA LIKE ''%/%'' AND
            LEN( SUBSTRING( a.PRODUTO_REFERENCIA, 1, CHARINDEX(''/'', a.PRODUTO_REFERENCIA ) ) ) < 5 AND
            LEN( RTRIM( LTRIM( a.PRODUTO_REFERENCIA ) ) ) < 30

        UPDATE a
        SET a.PRODUTO_REFERENCIATRANS = UPPER(dbo.fn_Remove_Caracteres_Especiais(a.PRODUTO_REFERENCIA))
        FROM
            ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG a
        WHERE
            a.Flag = 1 AND
            a.ProdutoMarca_MarcaCod IN (14,36,54)
    END
    ELSE
        PRINT ''  NÃO EXISTEM PRODUTOS DAS MARCAS FORD / VOLKS / MAN na Tabela Ficha_Prd_MG PARA AJUSTE DE REFERENCIA. ''
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' Atualiza Produto_Codigo na Tabela Ficha_Prd_MG'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.Produto_Codigo = b.Produto_Codigo
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.ProdutoMarca b ON (
        a.ProdutoMarca_MarcaCod = b.ProdutoMarca_MarcaCod AND
        rtrim(Ltrim(a.PRODUTO_REFERENCIA_Ajustado)) = rtrim(Ltrim(b.ProdutoMarca_Referencia)) collate database_default AND
        rtrim(Ltrim(a.PRODUTO_REFERENCIATRANS)) = rtrim(Ltrim(b.ProdutoMarca_ReferenciaAlfanumerico)) collate database_default
    )
    WHERE
        a.Flag = 1
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' AJUSTE de colunas VALOR com "","" na Tabela Ficha_Prd_MG'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.PRODUTO_QUANTIDADE = REPLACE(a.PRODUTO_QUANTIDADE,'','',''.'')
        ,a.VALOR_UNITARIO = REPLACE(a.VALOR_UNITARIO,'','',''.'')
        ,a.VALOR_DESCONTO = REPLACE(a.VALOR_DESCONTO,'','',''.'')
        ,a.VALOR_TOTAL = REPLACE(a.VALOR_TOTAL,'','',''.'')
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG a

    ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG ALTER COLUMN PRODUTO_QUANTIDADE FLOAT
    ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG ALTER COLUMN VALOR_UNITARIO FLOAT
    ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG ALTER COLUMN VALOR_DESCONTO FLOAT
    ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG ALTER COLUMN VALOR_TOTAL FLOAT
'
EXEC sp_executesql @CMD


PRINT '========================================================================================='
PRINT ' Atualiza Ocorrencia na Ficha_Prd_MG para VEÍCULOS SEM CADASTRO no WF'
PRINT '========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.Ocorrencia = a.Ocorrencia + '' | CADASTRAR VEÍCULO no WF.''
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG a
    WHERE
        a.Flag = 1 AND
        a.Veiculo_Codigo IS NULL
'
EXEC sp_executesql @CMD


PRINT '========================================================================================='
PRINT ' Atualiza Ocorrencia na Ficha_Prd_MG para PRODUTOS SEM CADASTRO no WF'
PRINT '========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.Ocorrencia = a.Ocorrencia + '' | CADASTRAR PRODUTOS no WF.''
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG a
    WHERE
        a.Flag = 1 AND
        a.Produto_Codigo IS NULL
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' VERIFICANDO CRITICAS DE EXTRAÇÃO DA TABELA Ficha_Prd_MG'
PRINT '=========================================================================================='

SELECT @CMD = '
    IF ( SELECT COUNT(*) FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG a WHERE a.Ocorrencia != '''' ) > 0
    BEGIN
        PRINT '' ENCONTROU CRITICAS DE EXTRAÇÃO DA TABELA <Ficha_Prd_MG>''

        SELECT
            ''Ficha_Prd_MG'' AS [TABELA],
            COUNT(*) AS [QTDE DE REGISTROS FLAG = 0 ]
        FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG a
        WHERE
            a.Flag = 0

        SELECT
            ''Ficha_Prd_MG'' AS [TABELA],
            COUNT(*) AS [QTDE DE REGISTROS COM OCORRÊNCIAS]
        FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG a
        WHERE
            a.Ocorrencia != ''''
    END
    ELSE
        PRINT '' NÃO ENCONTROU CRITICAS DE EXTRAÇÃO DA TABELA <Ficha_Prd_MG>''
'
EXEC sp_executesql @CMD
GO
-- <<< FIM: up_01_Extrai_Fseg_Prd_gx.sql
GO


-- >>> INICIO: up_01_Extrai_Fseg_Srv_gx.sql
-- =============================================================================
-- Layout: Fseg_Srv
-- Staging : Arquivo_FSeg_Srv_Tratado
-- Destino : Ficha_Srv_MG
-- Procedure: up_01_Extrai_Fseg_Srv_gx (@BancoDadosGX, @BancoWF)
-- Depende : Ficha_Cab_MG (layout 13 Fseg_Cab)
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Extrai_Fseg_Srv_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_01_Extrai_Fseg_Srv_gx;
GO

CREATE PROCEDURE dbo.up_01_Extrai_Fseg_Srv_gx
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX)
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

SELECT @CMD = N'
    IF NOT EXISTS (
        SELECT 1 FROM ' + QUOTENAME(LTRIM(RTRIM(@BancoDadosGX))) + N'.sys.objects
        WHERE type = ''U'' AND name = ''Arquivo_FSeg_Srv_Tratado''
    )
        SELECT @ok = 0
    ELSE
        SELECT @ok = 1
'
DECLARE @StagingOk BIT = 0
EXEC sp_executesql @CMD, N'@ok BIT OUTPUT', @ok = @StagingOk OUTPUT
IF @StagingOk = 0
BEGIN
    PRINT 'O < ARQUIVO > INFORMADO NAO EXISTE NESTE BANCO ' + @BancoDadosGX + '!'
    RETURN
END

PRINT '=========================================================================================='
PRINT ' CRIA a cópia do Arquivo_FSeg_Srv_Tratado para Migração'
PRINT '=========================================================================================='

SELECT @CMD = '
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects WHERE type = ''U'' AND name =''Ficha_Srv_MG'') )
    BEGIN
        SELECT a.* INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG
        FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Arquivo_FSeg_Srv_Tratado a
        WHERE 1 = 1
    END
'
EXEC sp_executesql @CMD

SELECT @CMD = '
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''Veiculo_Codigo'' AND obj.name = ''Ficha_Srv_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG ADD Veiculo_Codigo int NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''TMO_CodigoWF'' AND obj.name = ''Ficha_Srv_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG ADD TMO_CodigoWF int NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''TMO_DescricaoWF'' AND obj.name = ''Ficha_Srv_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG ADD TMO_DescricaoWF varchar(50) NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''Empresa_Codigo'' AND obj.name = ''Ficha_Srv_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG ADD Empresa_Codigo int NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''TipoOSCod'' AND obj.name = ''Ficha_Srv_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG ADD TipoOSCod int NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''Ocorrencia'' AND obj.name = ''Ficha_Srv_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG ADD Ocorrencia varchar(500) NULL
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' ATUALIZA Flag=0 na Tabela Ficha_Srv_MG CHASSI em BRANCO/NULO'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a SET a.Ocorrencia = '''' FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG a

    UPDATE a
    SET a.Ocorrencia = CASE
                        WHEN CHASSI IS NULL OR CHASSI = ''''
                        THEN a.Ocorrencia + ''CHASSI é BRANCO/NULO.''
                        ELSE a.Ocorrencia
                    END,
        a.Flag = CASE
                    WHEN CHASSI IS NULL OR CHASSI = '''' THEN 0
                    ELSE a.Flag
                END
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG a
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' ATUALIZA Flag=0 na Tabela Ficha_Srv_MG NUMERO_OS em BRANCO/NULO'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.Ocorrencia = CASE
                        WHEN NUMERO_OS IS NULL OR NUMERO_OS = ''''
                        THEN a.Ocorrencia + '' | NUMERO_OS é BRANCO/NULO.''
                        ELSE a.Ocorrencia
                    END,
        a.Flag = CASE
                    WHEN NUMERO_OS IS NULL OR NUMERO_OS = '''' THEN 0
                    ELSE a.Flag
                END
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG a
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' ATUALIZA Flag=0 na Tabela Ficha_Srv_MG TMO_REFERENCIA em BRANCO/NULO'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.Ocorrencia = CASE
                        WHEN TMO_REFERENCIA IS NULL OR TMO_REFERENCIA = ''''
                        THEN a.Ocorrencia + '' | TMO_REFERENCIA é BRANCO/NULO.''
                        ELSE a.Ocorrencia
                    END,
        a.Flag = CASE
                    WHEN TMO_REFERENCIA IS NULL OR TMO_REFERENCIA = '''' THEN 0
                    ELSE a.Flag
                END
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG a
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' ATUALIZA Flag=0 na Tabela Ficha_Srv_MG Chassi NÃO EXISTE na Ficha_Cab_MG'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.Flag = 0
        ,a.Ocorrencia = a.Ocorrencia + '' | Chassi NÃO EXISTE na Ficha_Cab_MG.''
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG a
    WHERE
        NOT EXISTS(
            SELECT 1
            FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG b
            WHERE
                a.CHASSI = b.CHASSI
        )
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' ATUALIZA Flag=0 na Tabela Ficha_Srv_MG TMO_QUANTIDADE = 0'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.Flag = 0
        ,a.Ocorrencia = a.Ocorrencia + '' | TMO_QUANTIDADE = 0.''
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG a
    WHERE
        a.Flag = 1 AND
        (TMO_QUANTIDADE IS NULL OR TMO_QUANTIDADE = ''''
        OR TRY_CAST(RTRIM(LTRIM(TMO_QUANTIDADE)) AS DECIMAL(18, 6)) = 0)
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' ATUALIZA Flag=0 na Tabela Ficha_Srv_MG devido o Flag = 0 na Ficha_Cab_MG'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.Flag = 0
        ,a.Ocorrencia = a.Ocorrencia + '' | Registro é FLAG=0 na Ficha_Cab_MG.''
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG a
    WHERE
        EXISTS(
            SELECT 1
            FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG b
            WHERE
                a.CHASSI = b.CHASSI AND
                a.NUMERO_OS = b.NUMERO_OS AND
                b.Flag = 0
        )
'
EXEC sp_executesql @CMD

/* VERIFICAR COM MAIS CALMA — duplicidade comentada no legado */


PRINT '=========================================================================================='
PRINT ' Ajusta campo CHASSI na Tabela Ficha_Srv_MG'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET
        a.CHASSI = UPPER(dbo.fn_Remove_Caracteres_Especiais(a.CHASSI))
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG a
    WHERE
        a.Flag = 1
'
EXEC sp_executesql @CMD


PRINT '========================================================================================='
PRINT ' ATUALIZA Ocorrencia na Ficha_Srv_MG para LEN(CHASSI)>17 OR LEN(CHASSI)<17'
PRINT '========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.Ocorrencia = a.Ocorrencia + '' | QTDE de Caracteres no CHASSI é INVÁLIDA.''
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG a
    WHERE
        a.Flag = 1 AND
        (LEN(a.CHASSI) > 17 OR LEN(CHASSI) < 17)
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' AJUSTE campo NUMERO_OS na Tabela Ficha_Srv_MG'
PRINT '=========================================================================================='

SELECT @CMD = '
    IF( SELECT COUNT(*) FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG WHERE ISNUMERIC(NUMERO_OS) = 0 ) > 0
    BEGIN
        UPDATE a
        SET a.NUMERO_OS = CASE
                            WHEN dbo.FN_RemoveCaracteresNaoInteiros(a.NUMERO_OS) IS NULL OR dbo.FN_RemoveCaracteresNaoInteiros(a.NUMERO_OS) = 0 THEN 1
                            ELSE dbo.FN_RemoveCaracteresNaoInteiros(a.NUMERO_OS)
                        END
        FROM
            ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG a
        WHERE
            a.Flag = 1
    END

    ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG ALTER COLUMN NUMERO_OS int;
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' ATUALIZA Veiculo_Codigo na tabela Ficha_Srv_MG'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a SET
        a.Veiculo_Codigo = b.Veiculo_Codigo
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Veiculo b ON a.CHASSI = b.Veiculo_Chassi COLLATE DATABASE_DEFAULT
    WHERE
        a.Flag = 1
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' ATUALIZA Empresa na tabela Ficha_Srv_MG'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.Empresa_Codigo = b.Empresa_Codigo
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG a
    INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Empresa_DePara b on b.Pessoa_DocIdentificador = a.CNPJ_EMPRESA collate database_default
    WHERE
        a.Flag = 1
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' ATUALIZA Flag=0 na Tabela Ficha_Srv_MG Empresa_Codigo em VAZIO/NULO'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.Flag = 0
        ,a.Ocorrencia = a.Ocorrencia + '' | Empresa_Codigo é VAZIO/NULO.''
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG a
    WHERE
        a.Flag = 1 AND
        a.Empresa_Codigo IS NULL
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' ATUALIZA TMO_REFERENCIA na tabela Ficha_Srv_MG'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.TMO_CodigoWF = TRY_CAST(RTRIM(LTRIM(a.TMO_REFERENCIA)) AS int)
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG a
    WHERE
        a.Flag = 1

    UPDATE a
    SET a.TMO_DescricaoWF = (SUBSTRING(a.TMO_DESCRICAO,1,50))
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG a
    WHERE
        a.Flag = 1
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' AJUSTE de colunas VALOR com "","" na Tabela Ficha_Srv_MG'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.TMO_QUANTIDADE = REPLACE(a.TMO_QUANTIDADE,'','',''.'')
        ,a.VALOR_UNITARIO = REPLACE(a.VALOR_UNITARIO,'','',''.'')
        ,a.VALOR_DESCONTO = REPLACE(a.VALOR_DESCONTO,'','',''.'')
        ,a.VALOR_TOTAL = REPLACE(a.VALOR_TOTAL,'','',''.'')
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG a

    ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG ALTER COLUMN TMO_QUANTIDADE FLOAT
    ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG ALTER COLUMN VALOR_UNITARIO FLOAT
    ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG ALTER COLUMN VALOR_DESCONTO FLOAT
    ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG ALTER COLUMN VALOR_TOTAL FLOAT
'
EXEC sp_executesql @CMD


PRINT '========================================================================================='
PRINT ' ATUALIZA Ocorrencia na Ficha_Srv_MG para VEÍCULOS SEM CADASTRO no WF'
PRINT '========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.Ocorrencia = a.Ocorrencia + '' | CADASTRAR VEÍCULO no WF.''
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG a
    WHERE
        a.Flag = 1 AND
        a.Veiculo_Codigo IS NULL
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' AJUSTE de colunas TMO_COBRADO na Tabela Ficha_Srv_MG'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.TMO_COBRADO = CASE
                            WHEN a.TMO_COBRADO IS NULL OR a.TMO_COBRADO = '''' THEN 1
                            WHEN a.TMO_COBRADO = ''S'' THEN 1
                            WHEN a.TMO_COBRADO = ''0'' OR a.TMO_COBRADO = 0 THEN 0
                            ELSE TRY_CAST(a.TMO_COBRADO AS int)
                        END
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG a

    ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG ALTER COLUMN TMO_COBRADO INT
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' VERIFICANDO CRITICAS DE EXTRAÇÃO DA TABELA Ficha_Srv_MG'
PRINT '=========================================================================================='

SELECT @CMD = '
    IF ( SELECT COUNT(*) FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG a WHERE a.Ocorrencia != '''' ) > 0
    BEGIN
        PRINT '' ENCONTROU CRITICAS DE EXTRAÇÃO DA TABELA <Ficha_Srv_MG>''

        SELECT
            ''Ficha_Srv_MG'' AS [TABELA],
            COUNT(*) AS [QTDE DE REGISTROS FLAG = 0 ]
        FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG a
        WHERE
            a.Flag = 0

        SELECT
            ''Ficha_Srv_MG'' AS [TABELA],
            COUNT(*) AS [QTDE DE REGISTROS COM OCORRÊNCIAS]
        FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG a
        WHERE
            a.Ocorrencia != ''''
    END
    ELSE
        PRINT '' NÃO ENCONTROU CRITICAS DE EXTRAÇÃO DA TABELA <Ficha_Srv_MG>''
'
EXEC sp_executesql @CMD
GO
-- <<< FIM: up_01_Extrai_Fseg_Srv_gx.sql
GO


-- >>> INICIO: up_01_Extrai_MovimentoEstoque_gx.sql
-- =============================================================================
-- Layout: MovimentoEstoque
-- Staging : Arquivo_MovimentoEstoque_Tratado
-- Destino : MovimentoEstoque_MG
-- Procedure: up_01_Extrai_MovimentoEstoque_gx (@BancoDadosGX, @BancoWF)
-- Autor: Aroldo Santana
-- Data de alteração: 07/07/2026
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Extrai_MovimentoEstoque_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_01_Extrai_MovimentoEstoque_gx;
GO

CREATE PROCEDURE dbo.up_01_Extrai_MovimentoEstoque_gx
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX)
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

IF NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoWF)
BEGIN
    PRINT 'O < ' + @BancoWF + ' > INFORMADO COMO @BancoWF NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

SELECT @CMD = N'
    IF NOT EXISTS (
        SELECT 1 FROM ' + QUOTENAME(LTRIM(RTRIM(@BancoDadosGX))) + N'.sys.objects
        WHERE type = ''U'' AND name = ''Arquivo_MovimentoEstoque_Tratado''
    )
        SELECT @ok = 0
    ELSE
        SELECT @ok = 1
'
DECLARE @StagingOk BIT = 0
EXEC sp_executesql @CMD, N'@ok BIT OUTPUT', @ok = @StagingOk OUTPUT
IF @StagingOk = 0
BEGIN
    PRINT 'O < ARQUIVO > INFORMADO NAO EXISTE NESTE BANCO ' + @BancoDadosGX + '!'
    RETURN
END

PRINT '=========================================================================================='
PRINT ' Cria a cópia do Arquivo_MovimentoEstoque_Tratado para Migração'
PRINT '=========================================================================================='

SELECT @CMD = '
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects WHERE type = ''U'' AND name = ''MovimentoEstoque_MG'') )
    BEGIN
        SELECT a.* INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.MovimentoEstoque_MG
        FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Arquivo_MovimentoEstoque_Tratado a
        WHERE 1 = 1
    END
'
EXEC sp_executesql @CMD

SELECT @CMD = '
    IF ( NOT EXISTS (SELECT 1
                     FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col
                     INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id
                     WHERE col.name = ''MovimentoEstoque_NaturezaOperacaoCod'' AND obj.name = ''MovimentoEstoque_MG'') )
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.MovimentoEstoque_MG ADD MovimentoEstoque_NaturezaOperacaoCod int NULL

    IF ( NOT EXISTS (SELECT 1
                     FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col
                     INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id
                     WHERE col.name = ''Estoque_CodigoWF'' AND obj.name = ''MovimentoEstoque_MG'') )
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.MovimentoEstoque_MG ADD Estoque_CodigoWF int NULL

    IF ( NOT EXISTS (SELECT 1
                     FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col
                     INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id
                     WHERE col.name = ''Departamento_CodigoWF'' AND obj.name = ''MovimentoEstoque_MG'') )
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.MovimentoEstoque_MG ADD Departamento_CodigoWF int NULL

    IF ( NOT EXISTS (SELECT 1
                     FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col
                     INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id
                     WHERE col.name = ''MovimentoEstoque_EmpresaCod'' AND obj.name = ''MovimentoEstoque_MG'') )
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.MovimentoEstoque_MG ADD MovimentoEstoque_EmpresaCod int NULL

    IF ( NOT EXISTS (SELECT 1
                     FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col
                     INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id
                     WHERE col.name = ''Produto_CodigoWF'' AND obj.name = ''MovimentoEstoque_MG'') )
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.MovimentoEstoque_MG ADD Produto_CodigoWF int NULL

    IF ( NOT EXISTS (SELECT 1
                     FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col
                     INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id
                     WHERE col.name = ''Referencia'' AND obj.name = ''MovimentoEstoque_MG'') )
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.MovimentoEstoque_MG ADD Referencia varchar(30) NULL

    IF ( NOT EXISTS (SELECT 1
                     FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col
                     INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id
                     WHERE col.name = ''ProdutoMarca_ReferenciaAlfanumerico'' AND obj.name = ''MovimentoEstoque_MG'') )
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.MovimentoEstoque_MG ADD ProdutoMarca_ReferenciaAlfanumerico varchar(30) NULL

    IF ( NOT EXISTS (SELECT 1
                     FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col
                     INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id
                     WHERE col.name = ''ProdutoMarca_MarcaCod'' AND obj.name = ''MovimentoEstoque_MG'') )
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.MovimentoEstoque_MG ADD ProdutoMarca_MarcaCod int NULL

    IF ( NOT EXISTS (SELECT 1
                     FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col
                     INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id
                     WHERE col.name = ''TipoProdutoCod'' AND obj.name = ''MovimentoEstoque_MG'') )
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.MovimentoEstoque_MG ADD TipoProdutoCod int NULL

    IF ( NOT EXISTS (SELECT 1
                     FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col
                     INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id
                     WHERE col.name = ''mult'' AND obj.name = ''MovimentoEstoque_MG'') )
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.MovimentoEstoque_MG ADD mult int NULL

    IF ( NOT EXISTS (SELECT 1
                     FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col
                     INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id
                     WHERE col.name = ''Pessoa_DocIdentificador'' AND obj.name = ''MovimentoEstoque_MG'') )
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.MovimentoEstoque_MG ADD Pessoa_DocIdentificador varchar(20) NULL

    IF ( NOT EXISTS (SELECT 1
                     FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col
                     INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id
                     WHERE col.name = ''MovimentoEstoque_PessoaCod'' AND obj.name = ''MovimentoEstoque_MG'') )
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.MovimentoEstoque_MG ADD MovimentoEstoque_PessoaCod int NULL

    IF ( NOT EXISTS (SELECT 1
                     FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col
                     INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id
                     WHERE col.name = ''Flag'' AND obj.name = ''MovimentoEstoque_MG'') )
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.MovimentoEstoque_MG ADD Flag int NULL

    IF ( NOT EXISTS (SELECT 1
                     FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col
                     INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id
                     WHERE col.name = ''Ocorrencia'' AND obj.name = ''MovimentoEstoque_MG'') )
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.MovimentoEstoque_MG ADD Ocorrencia varchar(500) NULL
'
EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' AJUSTE Pessoa_DocIdentificador na Tabela MovimentoEstoque_MG'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET
        a.Flag = ISNULL(a.Flag, 1),
        a.Ocorrencia = ISNULL(a.Ocorrencia, '''')
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.MovimentoEstoque_MG a

    UPDATE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.MovimentoEstoque_MG
    SET Pessoa_DocIdentificador = CASE
            WHEN LEN(RTRIM(LTRIM(CPF_CNPJ))) < 11
                THEN REPLICATE(''0'', 11 - LEN(RTRIM(LTRIM(CPF_CNPJ)))) + RTRIM(LTRIM(CPF_CNPJ))
            WHEN LEN(RTRIM(LTRIM(CPF_CNPJ))) > 11 AND LEN(RTRIM(LTRIM(CPF_CNPJ))) < 14
                THEN REPLICATE(''0'', 14 - LEN(RTRIM(LTRIM(CPF_CNPJ)))) + RTRIM(LTRIM(CPF_CNPJ))
            ELSE RTRIM(LTRIM(CPF_CNPJ))
        END
'
EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' AJUSTE MovimentoEstoque_PessoaCod na Tabela MovimentoEstoque_MG'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.MovimentoEstoque_PessoaCod = b.Pessoa_Codigo
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.MovimentoEstoque_MG a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Pessoa b
        ON a.Pessoa_DocIdentificador = b.Pessoa_DocIdentificador COLLATE database_default
'
EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' AJUSTE de colunas VALOR com "," na Tabela MovimentoEstoque_MG'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET
        a.QUANTIDADE = REPLACE(RTRIM(LTRIM(ISNULL(a.QUANTIDADE, ''''))), '','', ''.''),
        a.VALOR_UNITARIO = REPLACE(RTRIM(LTRIM(ISNULL(a.VALOR_UNITARIO, ''''))), '','', ''.''),
        a.VALOR_TOTAL = REPLACE(RTRIM(LTRIM(ISNULL(a.VALOR_TOTAL, ''''))), '','', ''.'')
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.MovimentoEstoque_MG a

    ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.MovimentoEstoque_MG ALTER COLUMN QUANTIDADE FLOAT
    ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.MovimentoEstoque_MG ALTER COLUMN VALOR_UNITARIO FLOAT
    ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.MovimentoEstoque_MG ALTER COLUMN VALOR_TOTAL FLOAT
'
EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' VERIFICAR DATA DE MOVIMENTO > DATAVIRADA na Tabela MovimentoEstoque_MG'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.Flag = 1
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.MovimentoEstoque_MG a
    INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Empresa_DePara b
        ON b.ParametroEmpresa_DataVirada < CAST(a.DATA_MOVIMENTO AS DATE)
    WHERE b.cg_cgccpf = a.CNPJ_EMPRESA
'
EXEC sp_executesql @CMD
GO
-- <<< FIM: up_01_Extrai_MovimentoEstoque_gx.sql
GO


-- >>> INICIO: up_01_Extrai_ProdLocacao_gx.sql
-- =============================================================================
-- Layout: ProdLocacao
-- Staging : Arquivo_ProdLocacao_Tratado
-- Destino : ProdLocacao_MG
-- Procedure: up_01_Extrai_ProdLocacao_gx (@BancoDadosGX, @BancoWF)
-- Sem De/Para
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Extrai_ProdLocacao_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_01_Extrai_ProdLocacao_gx;
GO

CREATE PROCEDURE dbo.up_01_Extrai_ProdLocacao_gx
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX)
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

SELECT @CMD = N'
    IF NOT EXISTS (
        SELECT 1 FROM ' + QUOTENAME(LTRIM(RTRIM(@BancoDadosGX))) + N'.sys.objects
        WHERE type = ''U'' AND name = ''Arquivo_ProdLocacao_Tratado''
    )
        SELECT @ok = 0
    ELSE
        SELECT @ok = 1
'
DECLARE @StagingOk BIT = 0
EXEC sp_executesql @CMD, N'@ok BIT OUTPUT', @ok = @StagingOk OUTPUT
IF @StagingOk = 0
BEGIN
    PRINT 'O < ARQUIVO > INFORMADO NAO EXISTE NESTE BANCO ' + @BancoDadosGX + '!'
    RETURN
END

PRINT '=========================================================================================='
PRINT ' CRIA a cópia do Arquivo_ProdLocacao_Tratado para Migração'
PRINT '=========================================================================================='

SELECT @CMD = '
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects WHERE type = ''U'' AND name = ''ProdLocacao_MG'') )
    BEGIN
        SELECT a.* INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ProdLocacao_MG
        FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Arquivo_ProdLocacao_Tratado a
        WHERE 1 = 1
    END
'
EXEC sp_executesql @CMD

SELECT @CMD = '
    IF ( NOT EXISTS (SELECT 1
                     FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col
                     INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id
                     WHERE col.name = ''Flag'' AND obj.name = ''ProdLocacao_MG'') )
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ProdLocacao_MG ADD Flag int NULL

    IF ( NOT EXISTS (SELECT 1
                     FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col
                     INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id
                     WHERE col.name = ''ProdutoEstoque_EmpresaCod'' AND obj.name = ''ProdLocacao_MG'') )
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ProdLocacao_MG ADD ProdutoEstoque_EmpresaCod int NULL

    IF ( NOT EXISTS (SELECT 1
                     FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col
                     INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id
                     WHERE col.name = ''ProdutoEstoque_EstoqueCod'' AND obj.name = ''ProdLocacao_MG'') )
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ProdLocacao_MG ADD ProdutoEstoque_EstoqueCod int NULL

    IF ( NOT EXISTS (SELECT 1
                     FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col
                     INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id
                     WHERE col.name = ''ProdutoMarca_MarcaCod'' AND obj.name = ''ProdLocacao_MG'') )
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ProdLocacao_MG ADD ProdutoMarca_MarcaCod int NULL

    IF ( NOT EXISTS (SELECT 1
                     FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col
                     INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id
                     WHERE col.name = ''PRODUTO_REFERENCIA_Ajustado'' AND obj.name = ''ProdLocacao_MG'') )
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ProdLocacao_MG ADD PRODUTO_REFERENCIA_Ajustado nvarchar(510) NULL

    IF ( NOT EXISTS (SELECT 1
                     FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col
                     INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id
                     WHERE col.name = ''PRODUTO_REFERENCIATRANS'' AND obj.name = ''ProdLocacao_MG'') )
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ProdLocacao_MG ADD PRODUTO_REFERENCIATRANS nvarchar(510) NULL

    IF ( NOT EXISTS (SELECT 1
                     FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col
                     INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id
                     WHERE col.name = ''Produto_CodigoWF'' AND obj.name = ''ProdLocacao_MG'') )
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ProdLocacao_MG ADD Produto_CodigoWF int NULL

    IF ( NOT EXISTS (SELECT 1
                     FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col
                     INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id
                     WHERE col.name = ''ProdutoEstoqueLocalizacao_LocalProdutoCod'' AND obj.name = ''ProdLocacao_MG'') )
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ProdLocacao_MG ADD ProdutoEstoqueLocalizacao_LocalProdutoCod int NULL

    IF ( NOT EXISTS (SELECT 1
                     FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col
                     INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id
                     WHERE col.name = ''ProdutoEstoqueLocalizacao_Tipo'' AND obj.name = ''ProdLocacao_MG'') )
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ProdLocacao_MG ADD ProdutoEstoqueLocalizacao_Tipo char(1) NULL

    IF ( NOT EXISTS (SELECT 1
                     FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col
                     INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id
                     WHERE col.name = ''Ocorrencia'' AND obj.name = ''ProdLocacao_MG'') )
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ProdLocacao_MG ADD Ocorrencia varchar(500) NULL
'
EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' ATUALIZA - Flag=0 na Tabela ProdLocacao_MG PRODUTO_REFERENCIA em VAZIO/NULO'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a SET a.Ocorrencia = '''' FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ProdLocacao_MG a

    UPDATE a
    SET a.Flag = ISNULL(a.Flag, 1),
        a.Ocorrencia = ISNULL(a.Ocorrencia, '''')

    UPDATE a
    SET a.Ocorrencia = CASE
            WHEN PRODUTO_REFERENCIA IS NULL OR RTRIM(LTRIM(PRODUTO_REFERENCIA)) = ''''
            THEN a.Ocorrencia + '' PRODUTO_REFERENCIA é VAZIO/NULO.''
            ELSE a.Ocorrencia
        END,
        a.Flag = CASE
            WHEN PRODUTO_REFERENCIA IS NULL OR RTRIM(LTRIM(PRODUTO_REFERENCIA)) = '''' THEN 0
            ELSE 1
        END
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ProdLocacao_MG a
'
EXEC sp_executesql @CMD

PRINT '===================================================================================================='
PRINT ' ATUALIZA - Empresa_Codigo, ProdutoMarca_MarcaCod, ProdutoEstoque_EstoqueCod na Tabela ProdLocacao_MG'
PRINT '===================================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.ProdutoEstoque_EmpresaCod = b.Empresa_Codigo,
        a.ProdutoMarca_MarcaCod = b.Empresa_MarcaCod
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ProdLocacao_MG a
    INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Empresa_DePara b
        ON b.Pessoa_DocIdentificador = a.CNPJ_EMPRESA COLLATE database_default
    WHERE a.Flag = 1

    UPDATE a
    SET a.ProdutoEstoque_EstoqueCod = (
        SELECT Estoque_Codigo FROM ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Estoque
        WHERE Estoque_Descricao = ''PE - PEÇAS E ACESSÓRIOS''
    )
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ProdLocacao_MG a
    WHERE a.Flag = 1

    UPDATE a
    SET a.Flag = 0
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ProdLocacao_MG a
    WHERE a.ProdutoEstoque_EmpresaCod IS NULL
'
EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' ATUALIZA - PRODUTO_REFERENCIA / PRODUTO_REFERENCIATRANS na tabela ProdLocacao_MG'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.PRODUTO_REFERENCIA_Ajustado = RTRIM(LTRIM(a.PRODUTO_REFERENCIA))
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ProdLocacao_MG a
    WHERE a.Flag = 1 AND a.ProdutoMarca_MarcaCod NOT IN (14,36,54)

    UPDATE a
    SET a.PRODUTO_REFERENCIATRANS = dbo.fn_Remove_Caracteres_Especiais(a.PRODUTO_REFERENCIA_Ajustado)
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ProdLocacao_MG a
    WHERE a.Flag = 1 AND a.ProdutoMarca_MarcaCod NOT IN (14,36,54)

    IF EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ProdLocacao_MG WHERE ProdutoMarca_MarcaCod IN (14,36,54))
    BEGIN
        UPDATE a
        SET a.PRODUTO_REFERENCIA_Ajustado = CASE
                WHEN LEN(REPLICATE('' '', 5 - LEN(SUBSTRING(a.PRODUTO_REFERENCIA, 1, CHARINDEX(''/'', a.PRODUTO_REFERENCIA)))) + a.PRODUTO_REFERENCIA) > 30
                    THEN a.PRODUTO_REFERENCIA
                ELSE REPLICATE('' '', 5 - LEN(SUBSTRING(a.PRODUTO_REFERENCIA, 1, CHARINDEX(''/'', a.PRODUTO_REFERENCIA)))) + a.PRODUTO_REFERENCIA
            END
        FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ProdLocacao_MG a
        WHERE a.Flag = 1 AND a.ProdutoMarca_MarcaCod IN (14,36,54)
          AND a.PRODUTO_REFERENCIA LIKE ''%/%''
          AND LEN(SUBSTRING(a.PRODUTO_REFERENCIA, 1, CHARINDEX(''/'', a.PRODUTO_REFERENCIA))) < 5
          AND LEN(RTRIM(LTRIM(a.PRODUTO_REFERENCIA))) < 30

        UPDATE a
        SET a.PRODUTO_REFERENCIATRANS = UPPER(dbo.fn_Remove_Caracteres_Especiais(a.PRODUTO_REFERENCIA))
        FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ProdLocacao_MG a
        WHERE a.Flag = 1 AND a.ProdutoMarca_MarcaCod IN (14,36,54)
    END
'
EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' ATUALIZA - Produto_CodigoWF na Tabela ProdLocacao_MG'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.Produto_CodigoWF = b.Produto_Codigo
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ProdLocacao_MG a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.ProdutoMarca b ON (
        a.ProdutoMarca_MarcaCod = b.ProdutoMarca_MarcaCod AND
        RTRIM(LTRIM(a.PRODUTO_REFERENCIA_Ajustado)) = RTRIM(LTRIM(b.ProdutoMarca_Referencia)) COLLATE database_default AND
        RTRIM(LTRIM(a.PRODUTO_REFERENCIATRANS)) = RTRIM(LTRIM(b.ProdutoMarca_ReferenciaAlfanumerico)) COLLATE database_default
    )
    WHERE a.Flag = 1
'
EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' ATUALIZA - Localização primária / secundária na Tabela ProdLocacao_MG'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.ProdutoEstoqueLocalizacao_LocalProdutoCod = b.LocalizacaoProduto_Codigo,
        a.ProdutoEstoqueLocalizacao_Tipo = ''P''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ProdLocacao_MG a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.LocalizacaoProduto b
        ON a.LOC_PRIMARIA = b.LocalizacaoProduto_Identificador COLLATE database_default
    WHERE a.Flag = 1 AND a.ProdutoEstoqueLocalizacao_LocalProdutoCod IS NULL

    UPDATE a
    SET a.ProdutoEstoqueLocalizacao_LocalProdutoCod = b.LocalizacaoProduto_Codigo,
        a.ProdutoEstoqueLocalizacao_Tipo = ''A''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ProdLocacao_MG a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.LocalizacaoProduto b
        ON a.LOC_SECUNDARIA = b.LocalizacaoProduto_Identificador COLLATE database_default
    WHERE a.Flag = 1 AND a.ProdutoEstoqueLocalizacao_LocalProdutoCod IS NULL
'
EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' Atualiza Flag quando referência não existe em Produto_MG'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET
        a.Flag = 0,
        a.Ocorrencia = ISNULL(a.Ocorrencia, '''') + '' | PRODUTO_REFERENCIA não encontrada em Produto_MG.''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ProdLocacao_MG a
    WHERE a.Flag = 1
      AND NOT EXISTS (
            SELECT 1
            FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG b
            WHERE UPPER(RTRIM(LTRIM(ISNULL(b.PRODUTO_REFERENCIA, ''''))))
                    = UPPER(RTRIM(LTRIM(ISNULL(a.PRODUTO_REFERENCIA, ''''))))
              AND dbo.fn_RemoveCaracteresNaoInteiros(RTRIM(LTRIM(ISNULL(b.CNPJ_EMPRESA, ''''))))
                    = dbo.fn_RemoveCaracteresNaoInteiros(RTRIM(LTRIM(ISNULL(a.CNPJ_EMPRESA, ''''))))
              AND ISNULL(b.Flag, 1) = 1
        )
'
EXEC sp_executesql @CMD
GO
-- <<< FIM: up_01_Extrai_ProdLocacao_gx.sql
GO


-- >>> INICIO: up_01_Extrai_Produto_gx.sql
-- =============================================================================
-- Layout: 7 Produto
-- Staging : Arquivo_Produto_Tratado
-- Destino : Produto_MG
-- Procedure: up_01_Extrai_Produto_gx (@BancoDadosGX, @BancoWF)
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Extrai_Produto_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_01_Extrai_Produto_gx;
GO

CREATE PROCEDURE dbo.up_01_Extrai_Produto_gx
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX)
BEGIN
    PRINT 'O < '+ @BancoDadosGX +' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

SELECT @CMD = N'
    IF NOT EXISTS (
        SELECT 1 FROM ' + QUOTENAME(LTRIM(RTRIM(@BancoDadosGX))) + N'.sys.objects
        WHERE type = ''U'' AND name = ''Arquivo_Produto_Tratado''
    )
        SELECT @ok = 0
    ELSE
        SELECT @ok = 1
'
DECLARE @StagingOk BIT = 0
EXEC sp_executesql @CMD, N'@ok BIT OUTPUT', @ok = @StagingOk OUTPUT
IF @StagingOk = 0
BEGIN
    PRINT 'O < ARQUIVO > INFORMADO NAO EXISTE NESTE BANCO '+ @BancoDadosGX +'!'
    RETURN
END

PRINT '=========================================================================================='
PRINT ' Cria a cópia do Arquivo_Produto_Tratado para Migração'
PRINT '=========================================================================================='

SELECT @CMD = '
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects WHERE type = ''U'' AND name =''Produto_MG'') )
    BEGIN
        SELECT a.* INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Arquivo_Produto_Tratado a WHERE 1=1
    END
'
EXEC sp_executesql @CMD

SELECT @CMD = '
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''Empresa_Codigo'' AND obj.name = ''Produto_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG ADD Empresa_Codigo int null
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''PRODUTO_REFERENCIA_Ajustado'' AND obj.name = ''Produto_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG ADD PRODUTO_REFERENCIA_Ajustado nvarchar(510) NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''PRODUTO_REFERENCIATRANS'' AND obj.name = ''Produto_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG ADD PRODUTO_REFERENCIATRANS nvarchar(510) NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''ProdutoMarca_MarcaCod'' AND obj.name = ''Produto_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG ADD ProdutoMarca_MarcaCod int null
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''Produto_CodigoWF'' AND obj.name = ''Produto_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG ADD Produto_CodigoWF int null
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''Ocorrencia'' AND obj.name = ''Produto_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG ADD Ocorrencia varchar(500) null
'
EXEC sp_executesql @CMD

SELECT @CMD = '
    UPDATE a SET a.Ocorrencia = '''' FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG a

    UPDATE a
    SET a.Ocorrencia = CASE
            WHEN PRODUTO_REFERENCIA IS NULL OR PRODUTO_REFERENCIA = ''''
            THEN a.Ocorrencia + '' PRODUTO_REFERENCIA é VAZIO/NULO.''
            ELSE a.Ocorrencia
        END,
        a.Flag = CASE
            WHEN PRODUTO_REFERENCIA IS NULL OR PRODUTO_REFERENCIA = '''' THEN 0
            ELSE 1
        END
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG a

    UPDATE a
    SET a.Ocorrencia = CASE
            WHEN PRODUTO_DESCRICAO IS NULL OR PRODUTO_DESCRICAO = ''''
            THEN a.Ocorrencia + '' PRODUTO_DESCRICAO é VAZIO/NULO.''
            ELSE a.Ocorrencia
        END,
        a.Flag = CASE
            WHEN PRODUTO_DESCRICAO IS NULL OR PRODUTO_DESCRICAO = '''' THEN 0
            ELSE 1
        END
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG a

    UPDATE a
    SET a.Empresa_Codigo = b.Empresa_Codigo
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG a
    INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Empresa_DePara b ON b.Pessoa_DocIdentificador = a.CNPJ_EMPRESA collate database_default
    WHERE a.Flag = 1

    UPDATE a
    SET a.ProdutoMarca_MarcaCod = b.Empresa_MarcaCod
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG a
    INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Empresa_DePara b on b.Empresa_Codigo = a.Empresa_Codigo
    WHERE a.Flag = 1
'
EXEC sp_executesql @CMD
GO
-- <<< FIM: up_01_Extrai_Produto_gx.sql
GO


-- >>> INICIO: up_02_Atualiza_Referencia_Produto_gx.sql
-- =============================================================================
-- Layout: 7 Produto
-- Staging : Produto_MG
-- Destino : Produto_MG
-- Procedure: up_02_Atualiza_Referencia_Produto_gx (@BancoDadosGX, @BancoWF)
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_02_Atualiza_Referencia_Produto_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_02_Atualiza_Referencia_Produto_gx;
GO

CREATE PROCEDURE dbo.up_02_Atualiza_Referencia_Produto_gx
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF (NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX))
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

PRINT '=========================================================================================='
PRINT ' Atualiza PRODUTO_REFERENCIA / PRODUTO_REFERENCIATRANS na tabela Produto_MG '
PRINT '=========================================================================================='

SELECT @CMD = '

    UPDATE a
    SET a.PRODUTO_REFERENCIA_Ajustado = RTRIM(LTRIM(a.PRODUTO_REFERENCIA))
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG a
    WHERE
        a.Flag = 1 AND
        a.ProdutoMarca_MarcaCod NOT IN (14,36,54)  -- FORD / VOLKS / MAN

    UPDATE a
    SET a.PRODUTO_REFERENCIATRANS = (dbo.fn_Remove_Caracteres_Especiais(a.PRODUTO_REFERENCIA_Ajustado))
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG a
    WHERE
        a.Flag = 1 AND
        a.ProdutoMarca_MarcaCod NOT IN (14,36,54)  -- FORD / VOLKS / MAN
'
EXEC sp_executesql @CMD

PRINT '========================================================================================================='
PRINT ' Atualiza PRODUTO_REFERENCIA / PRODUTO_REFERENCIATRANS das marcas FORD / VOLKS / MAN na Tabela Produto_MG'
PRINT '========================================================================================================='

SELECT @CMD = '

    IF EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG WHERE ProdutoMarca_MarcaCod IN (14,36,54))
    BEGIN
        UPDATE a
        SET a.PRODUTO_REFERENCIA_Ajustado = CASE
                WHEN LEN(REPLICATE('' '', 5 - LEN(SUBSTRING(a.PRODUTO_REFERENCIA, 1, CHARINDEX(''/'', a.PRODUTO_REFERENCIA)))) + a.PRODUTO_REFERENCIA) > 30
                    THEN a.PRODUTO_REFERENCIA
                ELSE REPLICATE('' '', 5 - LEN(SUBSTRING(a.PRODUTO_REFERENCIA, 1, CHARINDEX(''/'', a.PRODUTO_REFERENCIA)))) + a.PRODUTO_REFERENCIA
            END
        FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG a
        WHERE
            a.Flag = 1 AND
            a.ProdutoMarca_MarcaCod IN (14,36,54) AND
            a.PRODUTO_REFERENCIA LIKE ''%/%'' AND
            LEN(SUBSTRING(a.PRODUTO_REFERENCIA, 1, CHARINDEX(''/'', a.PRODUTO_REFERENCIA))) < 5 AND
            LEN(RTRIM(LTRIM(a.PRODUTO_REFERENCIA))) < ''30''

        UPDATE a
        SET a.PRODUTO_REFERENCIATRANS = UPPER(dbo.fn_Remove_Caracteres_Especiais(a.PRODUTO_REFERENCIA))
        FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG a
        WHERE
            a.Flag = 1 AND
            a.ProdutoMarca_MarcaCod IN (14,36,54)
    END
    ELSE
        PRINT ''  NAO EXISTEM PRODUTOS DAS MARCAS FORD / VOLKS / MAN NA Tabela Produto_MG PARA AJUSTE DE REFERENCIA. ''
'
EXEC sp_executesql @CMD

SELECT @CMD = '

PRINT ''=========================================================================================='' 
PRINT '' Atualiza Flag = 0 na Tabela Produto_MG ja Cadastrado em WF-Producao.'' 
PRINT ''==========================================================================================''

    UPDATE a
    SET a.Flag = 0,
        a.Ocorrencia = a.Ocorrencia + '' | Cadastrado em WF-Producao.'',
        a.Produto_CodigoWF = b.Produto_Codigo
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.ProdutoMarca b ON (
        a.ProdutoMarca_MarcaCod = b.ProdutoMarca_MarcaCod AND
        RTRIM(LTRIM(a.PRODUTO_REFERENCIA_Ajustado)) = RTRIM(LTRIM(b.ProdutoMarca_Referencia)) COLLATE database_default AND
        RTRIM(LTRIM(a.PRODUTO_REFERENCIATRANS)) = RTRIM(LTRIM(b.ProdutoMarca_ReferenciaAlfanumerico)) COLLATE database_default
    )
    WHERE
        a.Flag = 1
'
EXEC sp_executesql @CMD
GO
-- <<< FIM: up_02_Atualiza_Referencia_Produto_gx.sql
GO


-- >>> INICIO: up_03_Trata_Duplicidade_Produto_gx.sql
-- =============================================================================
-- Layout: 7 Produto
-- Staging : Produto_MG
-- Destino : Produto_MG
-- Procedure: up_03_Trata_Duplicidade_Produto_gx (@BancoDadosGX, @BancoWF)
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_03_Trata_Duplicidade_Produto_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_03_Trata_Duplicidade_Produto_gx;
GO

CREATE PROCEDURE dbo.up_03_Trata_Duplicidade_Produto_gx
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF (NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX))
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

PRINT '====================================================================================================='
PRINT '    TRATAMENTO de Duplicidades '
PRINT '====================================================================================================='

PRINT '    1 - PRODUTO_REFERENCIA DIFERENTE com PRODUTO_REFERENCIATRANS Igual na Produto_MG    '
PRINT '====================================================================================================='

SELECT @CMD = '

    IF EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects WHERE name = ''Ref_Diferente_Trans_Igual'' AND type = ''U'')
    BEGIN
        DROP TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ref_Diferente_Trans_Igual
    END

    SELECT DISTINCT
        RTRIM(LTRIM(a.CODIGO_PRODUTO)) as CODIGO_PRODUTO,
        a.PRODUTO_DESCRICAO as DESCRICAO_PRODUTO,
        a.PRODUTO_REFERENCIA,
        a.PRODUTO_REFERENCIA_Ajustado,
        a.PRODUTO_REFERENCIATRANS,
        a.MARCA_CODIGO,
        b.Produto_Codigo,
        b.Produto_Descricao,
        pm.ProdutoMarca_Referencia,
        pm.ProdutoMarca_ReferenciaAlfanumerico,
        pm.ProdutoMarca_MarcaCod
    INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ref_Diferente_Trans_Igual
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.ProdutoMarca pm ON
        a.ProdutoMarca_MarcaCod = pm.ProdutoMarca_MarcaCod AND
        RTRIM(LTRIM(a.PRODUTO_REFERENCIA_Ajustado)) != RTRIM(LTRIM(pm.ProdutoMarca_Referencia)) COLLATE database_default AND
        RTRIM(LTRIM(a.PRODUTO_REFERENCIATRANS)) = RTRIM(LTRIM(pm.ProdutoMarca_ReferenciaAlfanumerico)) COLLATE database_default
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Produto b ON pm.Produto_Codigo = b.Produto_Codigo
    WHERE
        a.Flag = 1

    UPDATE a
    SET a.Flag = 0,
        a.Ocorrencia = a.Ocorrencia + '' | PRODUTO_REFERENCIA DIFERENTE com PRODUTO_REFERENCIATRANS Igual.''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG a
    INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ref_Diferente_Trans_Igual b ON
        RTRIM(LTRIM(b.CODIGO_PRODUTO)) = RTRIM(LTRIM(a.CODIGO_PRODUTO)) AND
        a.PRODUTO_REFERENCIA_Ajustado = b.PRODUTO_REFERENCIA_Ajustado AND
        a.PRODUTO_REFERENCIATRANS = b.PRODUTO_REFERENCIATRANS AND
        a.ProdutoMarca_MarcaCod = b.ProdutoMarca_MarcaCod
'
EXEC sp_executesql @CMD

PRINT '    2 - PRODUTO_REFERENCIA Duplicada na Produto_MG    '
PRINT '====================================================================================================='

SELECT @CMD = '

    IF EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects WHERE name = ''Referencia_Duplicada'' AND TYPE = ''U'')
    BEGIN
        DROP TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Referencia_Duplicada
    END

    SELECT
        RTRIM(LTRIM(a.PRODUTO_REFERENCIA_Ajustado)) as PRODUTO_REFERENCIA_Ajustado,
        a.MARCA_CODIGO,
        a.ProdutoMarca_MarcaCod,
        COUNT(*) as QTD,
        MIN(IdTabela) as IdTabela
    INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Referencia_Duplicada
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG a
    WHERE
        a.Flag = 1
    GROUP BY
        RTRIM(LTRIM(a.PRODUTO_REFERENCIA_Ajustado)),
        a.MARCA_CODIGO,
        a.ProdutoMarca_MarcaCod
    HAVING COUNT(*) > 1

    UPDATE a
    SET a.Flag = 0,
        a.Ocorrencia = a.Ocorrencia + '' | PRODUTO_REFERENCIA DUPLICADA.''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG a
    INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Referencia_Duplicada b ON
        RTRIM(LTRIM(a.PRODUTO_REFERENCIA_Ajustado)) = RTRIM(LTRIM(b.PRODUTO_REFERENCIA_Ajustado)) AND
        a.ProdutoMarca_MarcaCod = b.ProdutoMarca_MarcaCod AND
        a.MARCA_CODIGO = b.MARCA_CODIGO
    WHERE
        a.IdTabela > b.IdTabela
'
EXEC sp_executesql @CMD

PRINT '    3 - PRODUTO_REFERENCIATRANS Duplicada na Produto_MG    '
PRINT '====================================================================================================='

SELECT @CMD = '

    IF EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects WHERE name = ''ReferenciaTRANS_Duplicada'' AND type = ''U'')
    BEGIN
        DROP TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ReferenciaTRANS_Duplicada
    END

    SELECT
        RTRIM(LTRIM(a.PRODUTO_REFERENCIATRANS)) as PRODUTO_REFERENCIATRANS,
        a.MARCA_CODIGO,
        a.ProdutoMarca_MarcaCod,
        COUNT(*) as QTD,
        MIN(a.IdTabela) as IdTabela
    INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ReferenciaTRANS_Duplicada
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG a
    WHERE
        a.Flag = 1
    GROUP BY
        RTRIM(LTRIM(a.PRODUTO_REFERENCIATRANS)),
        a.MARCA_CODIGO,
        a.ProdutoMarca_MarcaCod
    HAVING COUNT(*) > 1
'
EXEC sp_executesql @CMD

PRINT '    4 - Referencia + Descricao + Marca + CNPJ Empresa duplicados na Produto_MG    '
PRINT '====================================================================================================='

SELECT @CMD = '

    IF EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects WHERE name = ''Produto_RefDescMarcaCnpj_Duplicada'' AND type = ''U'')
    BEGIN
        DROP TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_RefDescMarcaCnpj_Duplicada
    END

    SELECT
        UPPER(RTRIM(LTRIM(a.PRODUTO_REFERENCIA))) as PRODUTO_REFERENCIA,
        UPPER(RTRIM(LTRIM(a.PRODUTO_DESCRICAO))) as PRODUTO_DESCRICAO,
        UPPER(RTRIM(LTRIM(a.MARCA_CODIGO))) as MARCA_CODIGO,
        RTRIM(LTRIM(' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.fn_RemoveCaracteresNaoInteiros(a.CNPJ_EMPRESA))) as CNPJ_EMPRESA,
        COUNT(*) as QTD,
        MIN(a.IdTabela) as IdTabela
    INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_RefDescMarcaCnpj_Duplicada
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG a
    WHERE
        a.Flag = 1
    GROUP BY
        UPPER(RTRIM(LTRIM(a.PRODUTO_REFERENCIA))),
        UPPER(RTRIM(LTRIM(a.PRODUTO_DESCRICAO))),
        UPPER(RTRIM(LTRIM(a.MARCA_CODIGO))),
        RTRIM(LTRIM(' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.fn_RemoveCaracteresNaoInteiros(a.CNPJ_EMPRESA)))
    HAVING COUNT(*) > 1

    UPDATE a
    SET a.Flag = 0,
        a.Ocorrencia = a.Ocorrencia + '' | PRODUTO DUPLICADO (Referencia + Descricao + Marca + CNPJ Empresa iguais).''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG a
    INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_RefDescMarcaCnpj_Duplicada b ON
        UPPER(RTRIM(LTRIM(a.PRODUTO_REFERENCIA))) = b.PRODUTO_REFERENCIA AND
        UPPER(RTRIM(LTRIM(a.PRODUTO_DESCRICAO))) = b.PRODUTO_DESCRICAO AND
        UPPER(RTRIM(LTRIM(a.MARCA_CODIGO))) = b.MARCA_CODIGO AND
        RTRIM(LTRIM(' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.fn_RemoveCaracteresNaoInteiros(a.CNPJ_EMPRESA))) = b.CNPJ_EMPRESA
    WHERE
        a.IdTabela > b.IdTabela
'
EXEC sp_executesql @CMD

PRINT '    FIM TRATAMENTO de Duplicidades *** ANALISAR CRITICAS ***'
PRINT '====================================================================================================='
GO
-- <<< FIM: up_03_Trata_Duplicidade_Produto_gx.sql
GO


-- >>> INICIO: up_04_Atualiza_Ocorrencia_Produto_gx.sql
-- =============================================================================
-- Layout: 7 Produto
-- Staging : Produto_MG
-- Destino : Produto_MG
-- Procedure: up_04_Atualiza_Ocorrencia_Produto_gx (@BancoDadosGX, @BancoWF)
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_04_Atualiza_Ocorrencia_Produto_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_04_Atualiza_Ocorrencia_Produto_gx;
GO

CREATE PROCEDURE dbo.up_04_Atualiza_Ocorrencia_Produto_gx
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF (NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX))
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

PRINT '=========================================================================================='
PRINT ' Atualiza Ocorrencia para VALORES VAZIO/NULO na Tabela Produto_MG '
PRINT '=========================================================================================='

SELECT @CMD = '

    UPDATE a
    SET a.Ocorrencia = CASE
            WHEN VALOR_VENDA IS NULL
                 OR VALOR_VENDA = ''''
                 OR TRY_CAST(RTRIM(LTRIM(VALOR_VENDA)) AS DECIMAL(18, 6)) = 0
            THEN a.Ocorrencia + '' | VALOR_VENDA e INVALIDO.''
            ELSE a.Ocorrencia
        END
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG a
    WHERE a.Flag = 1

    UPDATE a
    SET a.Ocorrencia = CASE
            WHEN VALOR_SUGERIDO IS NULL
                 OR VALOR_SUGERIDO = ''''
                 OR TRY_CAST(RTRIM(LTRIM(VALOR_SUGERIDO)) AS DECIMAL(18, 6)) = 0
            THEN a.Ocorrencia + '' | VALOR_SUGERIDO e INVALIDO.''
            ELSE a.Ocorrencia
        END
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG a
    WHERE a.Flag = 1

    UPDATE a
    SET a.Ocorrencia = CASE
            WHEN VALOR_AQUISICAO IS NULL
                 OR VALOR_AQUISICAO = ''''
                 OR TRY_CAST(RTRIM(LTRIM(VALOR_AQUISICAO)) AS DECIMAL(18, 6)) = 0
            THEN a.Ocorrencia + '' | VALOR_AQUISICAO e INVALIDO.''
            ELSE a.Ocorrencia
        END
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG a
    WHERE a.Flag = 1

    UPDATE a
    SET a.Ocorrencia = CASE
            WHEN VALOR_GARANTIA IS NULL
                 OR VALOR_GARANTIA = ''''
                 OR TRY_CAST(RTRIM(LTRIM(VALOR_GARANTIA)) AS DECIMAL(18, 6)) = 0
            THEN a.Ocorrencia + '' | VALOR_GARANTIA e INVALIDO.''
            ELSE a.Ocorrencia
          END
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG a
    WHERE a.Flag = 1
'
EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' Atualiza Ocorrencia na Tabela Produto_MG UNIDADE_PRODUTO_CODIGO em VAZIO/NULO '
PRINT '=========================================================================================='

SELECT @CMD = '

    UPDATE a
    SET a.Ocorrencia = CASE
            WHEN UNIDADE_PRODUTO_CODIGO IS NULL OR UNIDADE_PRODUTO_CODIGO = ''''
            THEN a.Ocorrencia + '' | UNIDADE_PRODUTO_CODIGO e VAZIO/NULO.''
            ELSE a.Ocorrencia
        END
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG a
    WHERE a.Flag = 1
'
EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' Atualiza Ocorrencia na Tabela Produto_MG TIPO_PRODUTO_CODIGO em VAZIO/NULO '
PRINT '=========================================================================================='

SELECT @CMD = '

    UPDATE a
    SET a.Ocorrencia = CASE
            WHEN TIPO_PRODUTO_CODIGO IS NULL OR TIPO_PRODUTO_CODIGO = ''''
            THEN a.Ocorrencia + '' | TIPO_PRODUTO_CODIGO e VAZIO/NULO.''
            ELSE a.Ocorrencia
        END
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG a
    WHERE a.Flag = 1
'
EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' Atualiza Ocorrencia na Tabela Produto_MG GRUPO_LUCRATIVIDADE_CODIGO em VAZIO/NULO '
PRINT '=========================================================================================='

SELECT @CMD = '

    UPDATE a
    SET a.Ocorrencia = CASE
            WHEN GRUPO_LUCRATIVIDADE_CODIGO IS NULL OR GRUPO_LUCRATIVIDADE_CODIGO = ''''
            THEN a.Ocorrencia + '' | GRUPO_LUCRATIVIDADE_CODIGO e VAZIO/NULO.''
            ELSE a.Ocorrencia
        END
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG a
    WHERE a.Flag = 1
'
EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' Atualiza Ocorrencia na Tabela Produto_MG GRUPO_PRODUTO_CODIGO em VAZIO/NULO '
PRINT '=========================================================================================='

SELECT @CMD = '

    UPDATE a
    SET a.Ocorrencia = CASE
            WHEN GRUPO_PRODUTO_CODIGO IS NULL OR GRUPO_PRODUTO_CODIGO = ''''
            THEN a.Ocorrencia + '' | GRUPO_PRODUTO_CODIGO e VAZIO/NULO.''
            ELSE a.Ocorrencia
        END
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG a
    WHERE a.Flag = 1
'
EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' Atualiza Ocorrencia na Tabela Produto_MG PROCEDENCIA_CODIGO em VAZIO/NULO '
PRINT '=========================================================================================='

SELECT @CMD = '

    UPDATE a
    SET a.Ocorrencia = CASE
            WHEN PROCEDENCIA_CODIGO IS NULL OR PROCEDENCIA_CODIGO = ''''
            THEN a.Ocorrencia + '' | PROCEDENCIA_CODIGO e VAZIO/NULO.''
            ELSE a.Ocorrencia
        END
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG a
    WHERE a.Flag = 1
'
EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' Atualiza PRODUTO_ORIGINAL na Tabela Produto_MG '
PRINT '=========================================================================================='

SELECT @CMD = '

    UPDATE a
    SET a.PRODUTO_ORIGINAL = CASE
            WHEN RTRIM(LTRIM(a.PRODUTO_ORIGINAL)) = ''S'' THEN 1
            WHEN RTRIM(LTRIM(a.PRODUTO_ORIGINAL)) = ''1'' THEN 1
            ELSE 0
        END
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG a
    WHERE a.Flag = 1
'
EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' Atualiza ENVIA_GARANTIA na Tabela Produto_MG '
PRINT '=========================================================================================='

SELECT @CMD = '

    UPDATE a
    SET a.ENVIA_GARANTIA = CASE
            WHEN RTRIM(LTRIM(a.ENVIA_GARANTIA)) = ''S'' THEN 1
            WHEN RTRIM(LTRIM(a.ENVIA_GARANTIA)) = ''1'' THEN 1
            ELSE 0
        END
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG a
    WHERE a.Flag = 1
'
EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' AJUSTE de colunas VALOR com "," na Tabela Produto_MG '
PRINT '=========================================================================================='

SELECT @CMD = '

    UPDATE a
    SET a.VALOR_VENDA = REPLACE(a.VALOR_VENDA,'','',''.''),
        a.VALOR_SUGERIDO = REPLACE(a.VALOR_SUGERIDO,'','',''.''),
        a.VALOR_AQUISICAO = REPLACE(a.VALOR_AQUISICAO,'','',''.''),
        a.VALOR_GARANTIA = REPLACE(a.VALOR_GARANTIA,'','',''.'')
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG a
    WHERE a.Flag = 1

    ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG ALTER COLUMN VALOR_VENDA FLOAT
    ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG ALTER COLUMN VALOR_SUGERIDO FLOAT
    ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG ALTER COLUMN VALOR_AQUISICAO FLOAT
    ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG ALTER COLUMN VALOR_GARANTIA FLOAT
'
EXEC sp_executesql @CMD
GO
-- <<< FIM: up_04_Atualiza_Ocorrencia_Produto_gx.sql
GO


-- >>> INICIO: up_01_Extrai_ProdutoEstoque_gx.sql
-- =============================================================================
-- Layout: 8 ProdutoEstoque
-- Staging : Arquivo_ProdutoEstoque_Tratado
-- Destino : ProdutoEstoque_MG
-- Procedure: up_01_Extrai_ProdutoEstoque_gx (@BancoDadosGX)
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Extrai_ProdutoEstoque_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_01_Extrai_ProdutoEstoque_gx;
GO

CREATE PROCEDURE dbo.up_01_Extrai_ProdutoEstoque_gx
    @BancoDadosGX VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX)
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

SELECT @CMD = N'
    IF NOT EXISTS (
        SELECT 1 FROM ' + QUOTENAME(LTRIM(RTRIM(@BancoDadosGX))) + N'.sys.objects
        WHERE type = ''U'' AND name = ''Arquivo_ProdutoEstoque_Tratado''
    )
        SELECT @ok = 0
    ELSE
        SELECT @ok = 1
'
DECLARE @StagingOk BIT = 0
EXEC sp_executesql @CMD, N'@ok BIT OUTPUT', @ok = @StagingOk OUTPUT
IF @StagingOk = 0
BEGIN
    PRINT 'O < ARQUIVO > INFORMADO NAO EXISTE NESTE BANCO ' + @BancoDadosGX + '!'
    RETURN
END

PRINT '=========================================================================================='
PRINT ' Cria a cópia do Arquivo_ProdutoEstoque_Tratado para Migração'
PRINT '=========================================================================================='

SELECT @CMD = '
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects WHERE type = ''U'' AND name = ''ProdutoEstoque_MG'') )
    BEGIN
        SELECT a.* INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ProdutoEstoque_MG
        FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Arquivo_ProdutoEstoque_Tratado a
        WHERE 1 = 1
    END
'
EXEC sp_executesql @CMD

SELECT @CMD = '
    IF ( NOT EXISTS (SELECT 1
                     FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col
                     INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id
                     WHERE col.name = ''Emp_Ds'' AND obj.name = ''ProdutoEstoque_MG'') )
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ProdutoEstoque_MG ADD Emp_Ds varchar(200) NULL

    IF ( NOT EXISTS (SELECT 1
                     FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col
                     INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id
                     WHERE col.name = ''ProdutoEstoque_EmpresaCod'' AND obj.name = ''ProdutoEstoque_MG'') )
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ProdutoEstoque_MG ADD ProdutoEstoque_EmpresaCod int NULL

    IF ( NOT EXISTS (SELECT 1
                     FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col
                     INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id
                     WHERE col.name = ''ProdutoEstoque_EstoqueCod'' AND obj.name = ''ProdutoEstoque_MG'') )
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ProdutoEstoque_MG ADD ProdutoEstoque_EstoqueCod int NULL

    IF ( NOT EXISTS (SELECT 1
                     FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col
                     INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id
                     WHERE col.name = ''Produto_Codigo'' AND obj.name = ''ProdutoEstoque_MG'') )
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ProdutoEstoque_MG ADD Produto_Codigo int NULL

    IF ( NOT EXISTS (SELECT 1
                     FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col
                     INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id
                     WHERE col.name = ''PRODUTO_REFERENCIA_Ajustado'' AND obj.name = ''ProdutoEstoque_MG'') )
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ProdutoEstoque_MG ADD PRODUTO_REFERENCIA_Ajustado varchar(30) NULL

    IF ( NOT EXISTS (SELECT 1
                     FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col
                     INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id
                     WHERE col.name = ''PRODUTO_REFERENCIATRANS'' AND obj.name = ''ProdutoEstoque_MG'') )
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ProdutoEstoque_MG ADD PRODUTO_REFERENCIATRANS varchar(30) NULL

    IF ( NOT EXISTS (SELECT 1
                     FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col
                     INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id
                     WHERE col.name = ''ProdutoMarca_MarcaCod'' AND obj.name = ''ProdutoEstoque_MG'') )
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ProdutoEstoque_MG ADD ProdutoMarca_MarcaCod int NULL

    IF ( NOT EXISTS (SELECT 1
                     FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col
                     INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id
                     WHERE col.name = ''Flag'' AND obj.name = ''ProdutoEstoque_MG'') )
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ProdutoEstoque_MG ADD Flag int NULL

    IF ( NOT EXISTS (SELECT 1
                     FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col
                     INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id
                     WHERE col.name = ''Ocorrencia'' AND obj.name = ''ProdutoEstoque_MG'') )
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ProdutoEstoque_MG ADD Ocorrencia varchar(500) NULL
'
EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' Aplica validações e ajustes em ProdutoEstoque_MG'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET
        a.Flag = ISNULL(a.Flag, 1),
        a.Ocorrencia = ''''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ProdutoEstoque_MG a

    UPDATE a
    SET a.QUANTIDADE = REPLACE(RTRIM(LTRIM(ISNULL(a.QUANTIDADE, ''''))), '','', ''.'')
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ProdutoEstoque_MG a
    WHERE a.QUANTIDADE IS NOT NULL

    UPDATE a
    SET a.PRECO_MEDIO = REPLACE(RTRIM(LTRIM(ISNULL(a.PRECO_MEDIO, ''''))), '','', ''.'')
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ProdutoEstoque_MG a
    WHERE a.PRECO_MEDIO IS NOT NULL

    UPDATE a
    SET a.ESTOQUE_IDEAL = REPLACE(RTRIM(LTRIM(ISNULL(a.ESTOQUE_IDEAL, ''''))), '','', ''.'')
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ProdutoEstoque_MG a
    WHERE a.ESTOQUE_IDEAL IS NOT NULL

    UPDATE a
    SET a.ESTOQUE_CRITICO = REPLACE(RTRIM(LTRIM(ISNULL(a.ESTOQUE_CRITICO, ''''))), '','', ''.'')
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ProdutoEstoque_MG a
    WHERE a.ESTOQUE_CRITICO IS NOT NULL

    UPDATE a
    SET a.ESTOQUE_MAXIMO = REPLACE(RTRIM(LTRIM(ISNULL(a.ESTOQUE_MAXIMO, ''''))), '','', ''.'')
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ProdutoEstoque_MG a
    WHERE a.ESTOQUE_MAXIMO IS NOT NULL

    UPDATE a
    SET
        a.Ocorrencia = a.Ocorrencia + '' | PRODUTO_REFERENCIA está vazio.'',
        a.Flag = 0
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ProdutoEstoque_MG a
    WHERE a.Flag = 1
      AND (a.PRODUTO_REFERENCIA IS NULL OR RTRIM(LTRIM(a.PRODUTO_REFERENCIA)) = '''')

    UPDATE a
    SET
        a.Ocorrencia = a.Ocorrencia + '' | QUANTIDADE igual a zero.'',
        a.Flag = 0
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ProdutoEstoque_MG a
    WHERE a.Flag = 1
      AND ISNULL(TRY_CONVERT(float, REPLACE(RTRIM(LTRIM(a.QUANTIDADE)), '','', ''.'')), 0) = 0

    UPDATE a
    SET
        a.Ocorrencia = a.Ocorrencia + '' | PRECO_MEDIO igual a zero.'',
        a.Flag = 0
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ProdutoEstoque_MG a
    WHERE a.Flag = 1
      AND ISNULL(TRY_CONVERT(float, REPLACE(RTRIM(LTRIM(a.PRECO_MEDIO)), '','', ''.'')), 0) = 0

    UPDATE a
    SET
        a.ProdutoEstoque_EmpresaCod = b.Empresa_Codigo,
        a.ProdutoMarca_MarcaCod = b.Empresa_MarcaCod,
        a.Emp_Ds = b.Empresa_NomeFantasia
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ProdutoEstoque_MG a
    INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Empresa_DePara b
        ON dbo.fn_RemoveCaracteresNaoInteiros(RTRIM(LTRIM(ISNULL(a.CNPJ_EMPRESA, ''''))))
        = dbo.fn_RemoveCaracteresNaoInteiros(RTRIM(LTRIM(ISNULL(b.Pessoa_DocIdentificador, ''''))))
    WHERE a.Flag = 1

    UPDATE a
    SET
        a.ProdutoEstoque_EstoqueCod = TRY_CONVERT(int, RTRIM(LTRIM(a.ESTOQUE_CODIGO)))
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ProdutoEstoque_MG a
    WHERE a.Flag = 1

    UPDATE a
    SET a.PRODUTO_REFERENCIA_Ajustado = RTRIM(LTRIM(a.PRODUTO_REFERENCIA))
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ProdutoEstoque_MG a
    WHERE
        a.Flag = 1 AND
        a.ProdutoMarca_MarcaCod NOT IN (14,36,54)

    UPDATE a
    SET a.PRODUTO_REFERENCIATRANS = dbo.fn_Remove_Caracteres_Especiais(a.PRODUTO_REFERENCIA_Ajustado)
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ProdutoEstoque_MG a
    WHERE
        a.Flag = 1 AND
        a.ProdutoMarca_MarcaCod NOT IN (14,36,54)

    IF EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ProdutoEstoque_MG WHERE ProdutoMarca_MarcaCod IN (14,36,54))
    BEGIN
        UPDATE a
        SET a.PRODUTO_REFERENCIA_Ajustado = CASE
                WHEN LEN(REPLICATE('' '', 5 - LEN(SUBSTRING(a.PRODUTO_REFERENCIA, 1, CHARINDEX(''/'', a.PRODUTO_REFERENCIA)))) + a.PRODUTO_REFERENCIA) > 30
                    THEN a.PRODUTO_REFERENCIA
                ELSE REPLICATE('' '', 5 - LEN(SUBSTRING(a.PRODUTO_REFERENCIA, 1, CHARINDEX(''/'', a.PRODUTO_REFERENCIA)))) + a.PRODUTO_REFERENCIA
            END
        FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ProdutoEstoque_MG a
        WHERE
            a.Flag = 1 AND
            a.ProdutoMarca_MarcaCod IN (14,36,54) AND
            a.PRODUTO_REFERENCIA LIKE ''%/%'' AND
            LEN(SUBSTRING(a.PRODUTO_REFERENCIA, 1, CHARINDEX(''/'', a.PRODUTO_REFERENCIA))) < 5 AND
            LEN(RTRIM(LTRIM(a.PRODUTO_REFERENCIA))) < 30

        UPDATE a
        SET a.PRODUTO_REFERENCIATRANS = UPPER(dbo.fn_Remove_Caracteres_Especiais(a.PRODUTO_REFERENCIA))
        FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ProdutoEstoque_MG a
        WHERE
            a.Flag = 1 AND
            a.ProdutoMarca_MarcaCod IN (14,36,54)
    END
'
EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' Atualiza Flag quando referência não existe em Produto_MG'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET
        a.Flag = 0,
        a.Ocorrencia = ISNULL(a.Ocorrencia, '''') + '' | PRODUTO_REFERENCIA não encontrada em Produto_MG.''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ProdutoEstoque_MG a
    WHERE
        a.Flag = 1
        AND NOT EXISTS (
            SELECT 1
            FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG b
            WHERE
                UPPER(RTRIM(LTRIM(ISNULL(b.PRODUTO_REFERENCIA, ''''))))
                    = UPPER(RTRIM(LTRIM(ISNULL(a.PRODUTO_REFERENCIA, ''''))))
                AND dbo.fn_RemoveCaracteresNaoInteiros(RTRIM(LTRIM(ISNULL(b.CNPJ_EMPRESA, ''''))))
                    = dbo.fn_RemoveCaracteresNaoInteiros(RTRIM(LTRIM(ISNULL(a.CNPJ_EMPRESA, ''''))))
        )
'
EXEC sp_executesql @CMD
GO
-- <<< FIM: up_01_Extrai_ProdutoEstoque_gx.sql
GO


-- >>> INICIO: up_01_Extrai_Veiculo_gx.sql
-- =============================================================================
-- Layout: Veiculo
-- Staging : Arquivo_Veiculo_Tratado
-- Destino : Veiculo_MG
-- Procedure: up_01_Extrai_Veiculo_gx (@BancoDadosGX, @BancoWF, @BancoWF_Prod opcional)
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Extrai_Veiculo_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_01_Extrai_Veiculo_gx;
GO

CREATE PROCEDURE dbo.up_01_Extrai_Veiculo_gx
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX),
    @BancoWF_Prod VARCHAR(MAX) = NULL
AS
DECLARE @CMD NVARCHAR(MAX)
DECLARE @AnoAtual CHAR(2) = RIGHT(CAST(YEAR(GETDATE()) AS VARCHAR(4)), 2)

IF NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX)
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

SELECT @CMD = N'
    IF NOT EXISTS (
        SELECT 1 FROM ' + QUOTENAME(LTRIM(RTRIM(@BancoDadosGX))) + N'.sys.objects
        WHERE type = ''U'' AND name = ''Arquivo_Veiculo_Tratado''
    )
        SELECT @ok = 0
    ELSE
        SELECT @ok = 1
'
DECLARE @StagingOk BIT = 0
EXEC sp_executesql @CMD, N'@ok BIT OUTPUT', @ok = @StagingOk OUTPUT
IF @StagingOk = 0
BEGIN
    PRINT 'O < ARQUIVO > INFORMADO NAO EXISTE NESTE BANCO ' + @BancoDadosGX + '!'
    RETURN
END

SELECT @CMD = '
PRINT ''==========================================================================================''
PRINT '' Extrai Veiculo - Atualiza CPF_CNPJ ''
PRINT ''==========================================================================================''

    UPDATE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Arquivo_Veiculo_Tratado SET
        CPF_CNPJ = RTRIM(LTRIM(dbo.FN_RemoveCaracteresNaoInteiros(CPF_CNPJ)))
'
EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' Cria a cópia do Arquivo_Veiculo_Tratado para Migração'
PRINT '=========================================================================================='

SELECT @CMD = '
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects WHERE type = ''U'' AND name = ''Veiculo_MG'') )
    BEGIN
        SELECT a.* INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG
        FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Arquivo_Veiculo_Tratado a
        WHERE 1 = 1
    END
'
EXEC sp_executesql @CMD

SELECT @CMD = '
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''Ve_FabMod'' AND obj.name = ''Veiculo_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG ADD Ve_FabMod varchar(10) NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''Marca_CodigoWF'' AND obj.name = ''Veiculo_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG ADD Marca_CodigoWF int NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''ModeloVeiculoWF'' AND obj.name = ''Veiculo_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG ADD ModeloVeiculoWF int NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''CorInternaWF'' AND obj.name = ''Veiculo_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG ADD CorInternaWF int NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''CorExternaWF'' AND obj.name = ''Veiculo_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG ADD CorExternaWF int NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''VeiculoAno'' AND obj.name = ''Veiculo_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG ADD VeiculoAno int NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''Veiculo_Status'' AND obj.name = ''Veiculo_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG ADD Veiculo_Status char(1) NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''Empresa_Codigo'' AND obj.name = ''Veiculo_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG ADD Empresa_Codigo int NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''VeiculoProprietario'' AND obj.name = ''Veiculo_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG ADD VeiculoProprietario int NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''Veiculo_PessoaCodConcessionaria'' AND obj.name = ''Veiculo_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG ADD Veiculo_PessoaCodConcessionaria int NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''Pessoa_DocIdentificador'' AND obj.name = ''Veiculo_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG ADD Pessoa_DocIdentificador varchar(20) NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''Veiculo_EstadoCod_Placa'' AND obj.name = ''Veiculo_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG ADD Veiculo_EstadoCod_Placa char(2) NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''Veiculo_MunicipioCod_Placa'' AND obj.name = ''Veiculo_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG ADD Veiculo_MunicipioCod_Placa int NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''WMI_VIN'' AND obj.name = ''Veiculo_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG ADD WMI_VIN varchar(3) NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''WMI_Fabricante'' AND obj.name = ''Veiculo_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG ADD WMI_Fabricante varchar(150) NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''Maquina_Implemento'' AND obj.name = ''Veiculo_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG ADD Maquina_Implemento int NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''Ocorrencia'' AND obj.name = ''Veiculo_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG ADD Ocorrencia varchar(500) NULL
'
EXEC sp_executesql @CMD

SELECT @CMD = '
    UPDATE a SET a.Ocorrencia = '''' FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a

    UPDATE a
    SET a.Ocorrencia = CASE
            WHEN CHASSI IS NULL OR CHASSI = '''' THEN a.Ocorrencia + ''CHASSI é BRANCO/NULO.''
            ELSE a.Ocorrencia
        END,
        a.Flag = CASE
            WHEN CHASSI IS NULL OR CHASSI = '''' THEN 0
            ELSE 1
        END
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a

    UPDATE a
    SET a.CHASSI = UPPER(dbo.fn_Remove_Caracteres_Especiais(a.CHASSI))
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
    WHERE a.Flag = 1
'
EXEC sp_executesql @CMD

IF @BancoWF_Prod IS NOT NULL AND LTRIM(RTRIM(@BancoWF_Prod)) <> ''
BEGIN
    SELECT @CMD = '
        IF(EXISTS(SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects WHERE name = ''Veiculo_EmProducao'' AND type = ''U''))
            DROP TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_EmProducao

        SELECT a.*
        INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_EmProducao
        FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
        WHERE EXISTS (
            SELECT 1 FROM ' + LTRIM(RTRIM(@BancoWF_Prod)) + '.dbo.Veiculo b
            WHERE a.CHASSI = b.Veiculo_Chassi COLLATE database_default
        )

        UPDATE a SET
            a.Flag = 0,
            a.Ocorrencia = a.Ocorrencia + '' Cadastrado em WF-Produção.''
        FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
        INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_EmProducao b ON a.CHASSI = b.CHASSI
    '
    EXEC sp_executesql @CMD
END

SELECT @CMD = '
    IF(EXISTS(SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects WHERE name = ''Chassis_Duplicados'' AND type = ''U''))
        DROP TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Chassis_Duplicados

    SELECT a.CHASSI, COUNT(a.CHASSI) AS QUANTIDADE, MIN(a.IDTabela) AS ID
    INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Chassis_Duplicados
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
    GROUP BY a.CHASSI
    HAVING COUNT(a.CHASSI) > 1

    UPDATE a
    SET a.Flag = 0,
        a.Ocorrencia = a.Ocorrencia + '' | Duplicidades de CHASSI.''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
    INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Chassis_Duplicados b ON a.CHASSI = b.CHASSI COLLATE database_default
    WHERE a.IDtabela > b.ID

    UPDATE a
    SET a.Pessoa_DocIdentificador = CASE
            WHEN LEN(RTRIM(LTRIM(CPF_CNPJ))) < 11
                THEN REPLICATE(''0'', 11 - LEN(RTRIM(LTRIM(CPF_CNPJ)))) + RTRIM(LTRIM(CPF_CNPJ))
            WHEN LEN(RTRIM(LTRIM(CPF_CNPJ))) > 11 AND LEN(RTRIM(LTRIM(CPF_CNPJ))) < 14
                THEN REPLICATE(''0'', 14 - LEN(RTRIM(LTRIM(CPF_CNPJ)))) + RTRIM(LTRIM(CPF_CNPJ))
            ELSE RTRIM(LTRIM(CPF_CNPJ))
        END
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
    WHERE a.Flag = 1

    IF (SELECT COUNT(*) FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG WHERE ISNUMERIC(KM) = 0) > 0
    BEGIN
        UPDATE a
        SET a.KM = CASE
                WHEN dbo.FN_RemoveCaracteresNaoInteiros(a.KM) IS NULL OR dbo.FN_RemoveCaracteresNaoInteiros(a.KM) = 0 THEN 1
                ELSE dbo.FN_RemoveCaracteresNaoInteiros(a.KM)
            END
        FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
        WHERE a.Flag = 1
    END

    UPDATE a
    SET a.SERIE = CASE
            WHEN a.SERIE <> '''' OR ISNUMERIC(a.SERIE) = 1 THEN dbo.fn_Remove_Caracteres_Especiais(a.SERIE)
            WHEN a.SERIE IS NULL OR a.SERIE = '''' THEN RIGHT(RTRIM(LTRIM(UPPER(a.CHASSI))), 8)
        END
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
    WHERE a.Flag = 1

    UPDATE a
    SET a.RENAVAM = dbo.fn_Remove_Caracteres_Especiais(a.RENAVAM)
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
    WHERE a.Flag = 1 AND a.RENAVAM <> ''''

    UPDATE a
    SET a.PLACA = CASE
            WHEN dbo.fn_Remove_Caracteres_Especiais(a.PLACA) = '''' THEN NULL
            ELSE a.PLACA
        END
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
    WHERE a.Flag = 1

    UPDATE a
    SET a.Veiculo_Status = CASE
            WHEN RTRIM(LTRIM(a.VEICULO_NOVO)) = ''S'' THEN ''N''
            ELSE ISNULL(a.VEICULO_NOVO, ''U'')
        END
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
    WHERE a.Flag = 1

    UPDATE a
    SET a.Veiculo_Status = CASE
            WHEN RTRIM(LTRIM(a.PLACA)) <> '''' THEN ''U''
            ELSE ISNULL(a.PLACA, ''N'')
        END
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
    WHERE a.Flag = 1 AND a.Veiculo_Status = ''''
'
EXEC sp_executesql @CMD

SELECT @CMD = '
    IF (SELECT COUNT(*) FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG WHERE LEN(ANO_FABRICACAO) <= 2) > 0
    BEGIN
        UPDATE a
        SET a.ANO_FABRICACAO = CASE
                WHEN (a.ANO_FABRICACAO >= ''00'' AND a.ANO_FABRICACAO <= ''' + LTRIM(RTRIM(@AnoAtual)) + ''') THEN ''20'' + a.ANO_FABRICACAO
                ELSE ''19'' + a.ANO_FABRICACAO
            END
        FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
        WHERE a.Flag = 1 AND LEN(a.ANO_FABRICACAO) <= 2
    END

    IF (SELECT COUNT(*) FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG WHERE LEN(ANO_MODELO) <= 2) > 0
    BEGIN
        UPDATE a
        SET a.ANO_MODELO = CASE
                WHEN (a.ANO_MODELO >= ''00'' AND a.ANO_MODELO <= ''' + LTRIM(RTRIM(@AnoAtual)) + ''') THEN ''20'' + a.ANO_MODELO
                ELSE ''19'' + a.ANO_MODELO
            END
        FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
        WHERE a.Flag = 1 AND LEN(a.ANO_MODELO) <= 2
    END

    UPDATE a
    SET a.Ve_FabMod = ANO_FABRICACAO + ''/'' + ANO_MODELO
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
    WHERE a.Flag = 1

    UPDATE a
    SET a.Ocorrencia = CASE
            WHEN a.ANO_FABRICACAO IS NULL OR a.ANO_FABRICACAO = '''' THEN a.Ocorrencia + '' | ANO_FABRICACAO é VAZIO/NULO.''
            ELSE a.Ocorrencia
        END
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
    WHERE a.Flag = 1

    UPDATE a
    SET a.Ocorrencia = CASE
            WHEN a.ANO_MODELO IS NULL OR a.ANO_MODELO = '''' THEN a.Ocorrencia + '' | ANO_MODELO é VAZIO/NULO.''
            ELSE a.Ocorrencia
        END
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
    WHERE a.Flag = 1

    UPDATE a
    SET a.DATA_VENDA = CASE
            WHEN a.DATA_VENDA IS NULL OR REPLACE(a.DATA_VENDA, ''/'', ''-'') = '''' OR ISDATE(a.DATA_VENDA) = 0 THEN ''1900-01-01''
            ELSE REPLACE(a.DATA_VENDA, ''/'', ''-'')
        END
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
    WHERE a.Flag = 1

    UPDATE a
    SET a.Ocorrencia = CASE
            WHEN COR_EXTERNA_CODIGO = '''' OR COR_EXTERNA_DESCRICAO = '''' THEN a.Ocorrencia + '' | COR_EXTERNA é VAZIA/NULA.''
            ELSE a.Ocorrencia
        END
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a

    UPDATE a
    SET a.WMI_VIN = SUBSTRING(a.CHASSI, 1, 3)
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
    WHERE a.Flag = 1

    UPDATE a
    SET a.WMI_Fabricante = wmi.WMI_Fabricante
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
    LEFT JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.WMI_Veiculo wmi ON wmi.WMI_VIN = a.WMI_VIN
    WHERE a.Flag = 1

    UPDATE a
    SET a.Marca_CodigoWF = m.Marca_Codigo
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
    LEFT JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Marca m ON LTRIM(RTRIM(m.Marca_Descricao)) = ISNULL(LTRIM(RTRIM(a.WMI_Fabricante)), '''') COLLATE Latin1_General_CI_AI
'
EXEC sp_executesql @CMD
GO
-- <<< FIM: up_01_Extrai_Veiculo_gx.sql
GO


-- >>> INICIO: up_09_Extrai_Criticas.sql
-- =============================================================================
-- Layout: Utilitario - Criticas
-- Staging : (multiplas tabelas _MG)
-- Destino : (consulta)
-- Procedure: up_09_Extrai_Criticas (@BancoDadosGX)
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_09_Extrai_Criticas' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_09_Extrai_Criticas;
GO

CREATE PROCEDURE dbo.up_09_Extrai_Criticas
	@BancoDadosGX		VARCHAR(MAX)
	
AS

DECLARE @CMD NVARCHAR(MAX)
-- ==========================================================================

	IF ( NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX) )
		BEGIN 
			PRINT 'O < BANCO DE DADOSGX > INFORMADO NAO EXISTE NESTE SERVIDOR!'
			RETURN
		END

-- ==========================================================================================

PRINT '=========================================================================================='
PRINT '	VERIFICANDO CRITICAS DE EXTRAÇÃO DAS TABELAS DE PESSOA'
PRINT '=========================================================================================='

DECLARE @NAME VARCHAR(200)
DECLARE @BANCO VARCHAR(200)

select @BANCO = @BancoDadosGX

DECLARE CUR CURSOR FOR
  SELECT name FROM	sys.objects WHERE name like 'Pessoa_%MG'
  UNION
  SELECT name FROM	sys.objects WHERE name like 'FichaCad_%MG'

OPEN CUR

FETCH NEXT FROM CUR INTO @NAME

WHILE @@FETCH_STATUS = 0
  BEGIN
      SET @CMD = '
		IF ( SELECT COUNT(*) FROM ' + RTRIM(LTRIM(@BANCO)) + '.dbo.' + RTRIM(LTRIM(@NAME)) + ' a WHERE a.Ocorrencia != '''' ) > 0 
		BEGIN
			PRINT '' ENCONTROU CRITICAS DE EXTRAÇÃO DA TABELA <' + RTRIM(LTRIM(@NAME)) + '>''
			
			SELECT a.Ocorrencia,* FROM ' + RTRIM(LTRIM(@BANCO)) + '.dbo.' + RTRIM(LTRIM(@NAME)) + ' a WHERE a.Flag = 0
			
			UNION			
			
			SELECT a.Ocorrencia,* FROM ' + RTRIM(LTRIM(@BANCO)) + '.dbo.' + RTRIM(LTRIM(@NAME)) + ' a WHERE a.Ocorrencia != ''''

			SELECT 
				''' + RTRIM(LTRIM(@NAME)) + ''' AS [TABELA],
				COUNT(*) AS [QTDE DE REGISTROS FLAG = 0 ]				 
			FROM ' + RTRIM(LTRIM(@BANCO)) + '.dbo.' + RTRIM(LTRIM(@NAME)) + ' a
			WHERE 
				a.Flag = 0
			
			SELECT 
				''' + RTRIM(LTRIM(@NAME)) + ''' AS [TABELA],
				COUNT(*) AS [QTDE DE REGISTROS COM OCORRÊNCIAS]				 
			FROM ' + RTRIM(LTRIM(@BANCO)) + '.dbo.' + RTRIM(LTRIM(@NAME)) + ' a 
			WHERE 
				a.Ocorrencia != ''''

		END
		ELSE
			PRINT '' NÃO ENCONTROU CRITICAS DE EXTRAÇÃO DA TABELA <' + RTRIM(LTRIM(@NAME)) + '>''
		'
      --PRINT @CMD
      EXEC Sp_executesql @CMD
	FETCH NEXT FROM CUR INTO @NAME
  END

CLOSE CUR
DEALLOCATE CUR

GO
-- <<< FIM: up_09_Extrai_Criticas.sql
GO
