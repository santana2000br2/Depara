-- =============================================================================
-- INSTALAR COMPLETO — Extração + De/Para (DadosGX)
-- Instalador em T-SQL puro (NAO precisa SQLCMD Mode).
-- Antes de executar: substitua [DadosGX_SeuProjeto] pelo nome real do banco.
-- Gerado em: 01/10/2026 08:20
-- =============================================================================

USE [DadosGX_SeuProjeto];  -- ALTERE para o nome real do banco DadosGX
GO


-- >>> INICIO: 00_drop_procedures.sql
-- =============================================================================
-- DROP — Procedures de Extração (DadosGX)
-- Pacote DadosGX — gerar drop antes da reinstalacao
-- Gerado em: 01/10/2026 08:20
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Extrai_Adiantamento_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_01_Extrai_Adiantamento_gx];
GO

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


-- >>> INICIO: up_01_Extrai_Adiantamento_gx.sql
-- =============================================================================
-- Layout: Adiantamento
-- Staging : Arquivo_Adiantamento_Tratado
-- Destino : FichaRazao_MG
-- Procedure: up_01_Extrai_Adiantamento_gx (@BancoDadosGX, @BancoWF)
-- Depende : Pessoa_MG (CPF_CNPJ), Empresa_DePara (CNPJ_EMPRESA)
--
-- Campos extras (script de origem):
--   SALDO_Original float, Pessoa_DocIdentificador varchar(20),
--   TipoFichaRazao_Codigo int, FichaRazao_PessoaCod int,
--   FichaRazao_EmpresaCod int, Ocorrencia varchar(500), Flag
-- Layout do arquivo (não alterar): VALOR_SALDO (= SALDO), VALOR_ORIGINAL (= VALOR)
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Extrai_Adiantamento_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_01_Extrai_Adiantamento_gx;
GO

CREATE PROCEDURE dbo.up_01_Extrai_Adiantamento_gx
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF ( NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX) )
BEGIN
    PRINT 'O < BANCO DE DADOSGX > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

SELECT @CMD = N'
    IF NOT EXISTS (
        SELECT 1 FROM ' + QUOTENAME(LTRIM(RTRIM(@BancoDadosGX))) + N'.sys.objects
        WHERE type = ''U'' AND name = ''Arquivo_Adiantamento_Tratado''
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
PRINT GETDATE()
PRINT '=========================================================================================='

PRINT '=========================================================================================='
PRINT ' Extrai_Titulo - Atualiza CPFCNPJ '
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Arquivo_Adiantamento_Tratado SET
        CPF_CNPJ = RTRIM(LTRIM(dbo.FN_RemoveCaracteresNaoInteiros(CPF_CNPJ))),
        TIPO_MOVFINANCEIRO = UPPER(RTRIM(LTRIM(TIPO_MOVFINANCEIRO)))
'
EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' Criando a tabela FichaRazao_MG DadosGX '
PRINT '=========================================================================================='

SELECT @CMD = '
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects WHERE type = ''U'' AND name =''FichaRazao_MG'') )
    BEGIN
        SELECT a.* INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.FichaRazao_MG
        FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Arquivo_Adiantamento_Tratado a
        WHERE 1=1
    END

    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''Flag'' AND obj.name = ''FichaRazao_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.FichaRazao_MG ADD Flag smallint NULL
    UPDATE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.FichaRazao_MG SET Flag = 1 WHERE Flag IS NULL

    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''SALDO_Original'' AND obj.name = ''FichaRazao_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.FichaRazao_MG ADD SALDO_Original float null
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''Pessoa_DocIdentificador'' AND obj.name = ''FichaRazao_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.FichaRazao_MG ADD Pessoa_DocIdentificador varchar(20) null
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''TipoFichaRazao_Codigo'' AND obj.name = ''FichaRazao_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.FichaRazao_MG ADD TipoFichaRazao_Codigo int null
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''FichaRazao_PessoaCod'' AND obj.name = ''FichaRazao_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.FichaRazao_MG ADD FichaRazao_PessoaCod int null
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''FichaRazao_EmpresaCod'' AND obj.name = ''FichaRazao_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.FichaRazao_MG ADD FichaRazao_EmpresaCod int null
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''Ocorrencia'' AND obj.name = ''FichaRazao_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.FichaRazao_MG ADD Ocorrencia varchar(500) null
'
EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' AJUSTE de colunas VALOR com "," na Tabela FichaRazao_MG '
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.VALOR_SALDO = REPLACE(a.VALOR_SALDO,'','',''.''),
        a.VALOR_ORIGINAL = REPLACE(a.VALOR_ORIGINAL,'','',''.'')
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.FichaRazao_MG a

    UPDATE a SET
        a.VALOR_SALDO = REPLACE(a.VALOR_SALDO,''-'',''''),
        a.VALOR_ORIGINAL = REPLACE(a.VALOR_ORIGINAL,''-'','''')
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.FichaRazao_MG a
    WHERE
        a.VALOR_SALDO like ''%-%'' OR a.VALOR_ORIGINAL like ''%-%''

    UPDATE a SET
        a.VALOR_SALDO = NULL
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.FichaRazao_MG a
    WHERE RTRIM(LTRIM(ISNULL(CONVERT(varchar(50), a.VALOR_SALDO), ''''))) = ''''

    UPDATE a SET
        a.VALOR_ORIGINAL = NULL
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.FichaRazao_MG a
    WHERE RTRIM(LTRIM(ISNULL(CONVERT(varchar(50), a.VALOR_ORIGINAL), ''''))) = ''''

    UPDATE a SET
        a.SALDO_Original = TRY_CONVERT(float, a.VALOR_SALDO)
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.FichaRazao_MG a

    ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.FichaRazao_MG ALTER COLUMN VALOR_SALDO FLOAT
    ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.FichaRazao_MG ALTER COLUMN VALOR_ORIGINAL FLOAT
'
EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' Atualiza Flag=0 na Tabela FichaRazao_MG de EMPRESA não existe Empresa_DePara '
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a SET a.Ocorrencia = '''' FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.FichaRazao_MG a

    UPDATE a
    SET a.Ocorrencia = a.Ocorrencia + '' CNPJ_EMPRESA não existe Empresa_DePara. ''
        ,a.Flag = 0
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.FichaRazao_MG a
    WHERE
        NOT EXISTS(
            SELECT 1
            FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Empresa_DePara b
            WHERE b.Pessoa_DocIdentificador = a.CNPJ_EMPRESA COLLATE DATABASE_DEFAULT
        )
'
EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' Atualiza Flag=0 na Tabela FichaRazao_MG CPF/CNPJ em BRANCO '
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
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.FichaRazao_MG a
'
EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' Atualiza Flag=0 na Tabela FichaRazao_MG TIPO_MOVFINANCEIRO inválido'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.Flag = 0
        ,a.Ocorrencia = a.Ocorrencia + '' | TIPO_MOVFINANCEIRO deve ser R ou P.''
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.FichaRazao_MG a
    WHERE
        a.Flag = 1 AND
        ISNULL(a.TIPO_MOVFINANCEIRO, '''') NOT IN (''R'', ''P'')
'
EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' Atualiza Flag=0 na Tabela FichaRazao_MG TIPO_FICHARAZAO em BRANCO'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.Flag = 0
        ,a.Ocorrencia = a.Ocorrencia + '' | TIPO_FICHARAZAO está vazio.''
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.FichaRazao_MG a
    WHERE
        a.Flag = 1 AND
        (a.TIPO_FICHARAZAO IS NULL OR RTRIM(LTRIM(a.TIPO_FICHARAZAO)) = '''')
'
EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' Atualiza Flag=0 na Tabela FichaRazao_MG SALDO_Original = 0 '
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.Flag = 0
        ,a.Ocorrencia = a.Ocorrencia + '' | SALDO_Original = 0.''
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.FichaRazao_MG a
    WHERE
        a.Flag = 1 AND
        (a.SALDO_Original IS NULL OR a.SALDO_Original = 0)
'
EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' Atualiza Empresa na tabela FichaRazao_MG '
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.FichaRazao_MG SET
        CNPJ_EMPRESA = RTRIM(LTRIM(dbo.FN_RemoveCaracteresNaoInteiros(CNPJ_EMPRESA)))

    UPDATE a
    SET a.FichaRazao_EmpresaCod = b.Empresa_Codigo
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.FichaRazao_MG a
    INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Empresa_DePara b on b.Pessoa_DocIdentificador = a.CNPJ_EMPRESA COLLATE DATABASE_DEFAULT
    WHERE
        a.Flag = 1
'
EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' AJUSTE Pessoa_DocIdentificador na Tabela FichaRazao_MG '
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
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.FichaRazao_MG a
    WHERE
        a.Flag = 1
'
EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' Atualiza Pessoa na tabela FichaRazao_MG '
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.FichaRazao_PessoaCod = b.Pessoa_Codigo
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.FichaRazao_MG a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Pessoa b on b.Pessoa_DocIdentificador = a.Pessoa_DocIdentificador COLLATE DATABASE_DEFAULT
    WHERE
        a.Flag = 1
'
EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' Consulta Pessoa_DocIdentificador NÃO CADASTRADO '
PRINT '=========================================================================================='

SELECT @CMD = '
    IF(EXISTS(SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects WHERE name = ''PessoaFicha_Cadastrar'' AND type = ''U''))
    BEGIN
        DROP TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaFicha_Cadastrar
    END

    SELECT DISTINCT
        a.CPF_CNPJ
    INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaFicha_Cadastrar
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.FichaRazao_MG a
    WHERE
        a.Flag = 1 AND
        a.FichaRazao_PessoaCod IS NULL

    UPDATE a SET
        a.Ocorrencia = a.Ocorrencia + '' | Cadastrado de Pessoa não encontrado.''
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.FichaRazao_MG a
    WHERE
        a.Flag = 1 AND
        a.FichaRazao_PessoaCod IS NULL
'
EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' Ajusta DATA_MOVIMENTO na Tabela FichaRazao_MG'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a SET
        a.DATA_MOVIMENTO = CASE WHEN a.DATA_MOVIMENTO IS NULL OR LTRIM(RTRIM(CONVERT(varchar(30), a.DATA_MOVIMENTO, 23))) = '''' THEN CONVERT(varchar(30), a.DATA_MOVIMENTO) ELSE COALESCE(CONVERT(varchar(10), TRY_CONVERT(date, REPLACE(REPLACE(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), a.DATA_MOVIMENTO, 23))), 10), ''-'', ''/''), ''.'', ''/''), 103), 23),CONVERT(varchar(10), TRY_CONVERT(date, REPLACE(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), a.DATA_MOVIMENTO, 23))), 10), ''/'', ''-''), 23), 23),CONVERT(varchar(10), TRY_CONVERT(date, LTRIM(RTRIM(CONVERT(varchar(30), a.DATA_MOVIMENTO, 23))), 112), 23),CASE WHEN LEN(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), a.DATA_MOVIMENTO, 23))), 10)) <= 8 THEN CONVERT(varchar(10), TRY_CONVERT(date, REPLACE(REPLACE(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), a.DATA_MOVIMENTO, 23))), 10), ''-'', ''/''), ''.'', ''/''), 3), 23) END,''1900-01-01'') END
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.FichaRazao_MG a
    WHERE
        a.Flag = 1
'
EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' VERIFICANDO CRITICAS DE EXTRAÇÃO DA TABELA DE FICHA RAZAO'
PRINT '=========================================================================================='

SELECT @CMD = '
    IF ( SELECT COUNT(*) FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.FichaRazao_MG a WHERE a.Ocorrencia != '''' ) > 0
    BEGIN
        PRINT '' ENCONTROU CRITICAS DE EXTRAÇÃO DA TABELA <FichaRazao_MG>''

        SELECT
            ''FichaRazao_MG'' AS [TABELA],
            COUNT(*) AS [QTDE DE REGISTROS FLAG = 0 ]
        FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.FichaRazao_MG a
        WHERE
            a.Flag = 0

        SELECT
            ''FichaRazao_MG'' AS [TABELA],
            COUNT(*) AS [QTDE DE REGISTROS COM OCORRÊNCIAS]
        FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.FichaRazao_MG a
        WHERE
            a.Ocorrencia != ''''
    END
    ELSE
        PRINT '' NÃO ENCONTROU CRITICAS DE EXTRAÇÃO DA TABELA <FichaRazao_MG>''
'
EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' **ADT FORNECEDOR** VALIDAR SALDO ANTES DE PROSSEGUIR COM A MIGRAÇÃO DA TITULO'
PRINT '=========================================================================================='

SELECT @CMD = '
    SELECT
        ''ADT FORNECEDOR'' as ADT_FORNECEDOR
        ,a.CNPJ_EMPRESA
        ,b.Empresa_NomeFantasia
        ,COUNT(a.CNPJ_EMPRESA) as QTDE
        ,SUM(a.VALOR_SALDO) as SALDO
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.FichaRazao_MG a
    INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Empresa_DePara b on b.Pessoa_DocIdentificador = a.CNPJ_EMPRESA
    WHERE
        a.TIPO_MOVFINANCEIRO = ''P''
    GROUP BY
        a.CNPJ_EMPRESA,
        b.Empresa_NomeFantasia
    ORDER BY
        a.CNPJ_EMPRESA
'
EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' **ADT CLIENTE** VALIDAR SALDO ANTES DE PROSSEGUIR COM A MIGRAÇÃO DA TITULO'
PRINT '=========================================================================================='

SELECT @CMD = '
    SELECT
        ''ADT CLIENTE'' as ADT_CLIENTE
        ,a.CNPJ_EMPRESA
        ,b.Empresa_NomeFantasia
        ,COUNT(a.CNPJ_EMPRESA) as QTDE
        ,SUM(a.VALOR_SALDO) as SALDO
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.FichaRazao_MG a
    INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Empresa_DePara b on b.Pessoa_DocIdentificador = a.CNPJ_EMPRESA
    WHERE
        a.TIPO_MOVFINANCEIRO = ''R''
    GROUP BY
        a.CNPJ_EMPRESA,
        b.Empresa_NomeFantasia
    ORDER BY
        a.CNPJ_EMPRESA
'
EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' CADASTRAR Pessoa '
PRINT '=========================================================================================='

SELECT @CMD = '
    IF(SELECT COUNT(*) FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaFicha_Cadastrar) > 0
    BEGIN
        PRINT '' ** LISTA DE CPF_CNPJ para CADASTRO de Pessoa ''
        SELECT * FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaFicha_Cadastrar a
    END
'
EXEC sp_executesql @CMD
GO
-- <<< FIM: up_01_Extrai_Adiantamento_gx.sql
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

SELECT @CMD = N'
    UPDATE ' + QUOTENAME(LTRIM(RTRIM(@BancoDadosGX))) + N'.dbo.Arquivo_Financeiro_Tratado SET
        CPF_CNPJ = RTRIM(LTRIM(dbo.FN_RemoveCaracteresNaoInteiros(CPF_CNPJ)))
'
EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' Cria a cópia do Arquivo_Financeiro_Tratado para Migração'
PRINT '=========================================================================================='

SELECT @CMD = N'
    IF ( NOT EXISTS (SELECT 1 FROM ' + QUOTENAME(LTRIM(RTRIM(@BancoDadosGX))) + N'.sys.objects WHERE type = ''U'' AND name =''Titulo_MG'') )
    BEGIN
        SELECT a.* INTO ' + QUOTENAME(LTRIM(RTRIM(@BancoDadosGX))) + N'.dbo.Titulo_MG
        FROM ' + QUOTENAME(LTRIM(RTRIM(@BancoDadosGX))) + N'.dbo.Arquivo_Financeiro_Tratado a
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
        a.DATA_EMISSAO = CASE WHEN a.DATA_EMISSAO IS NULL OR LTRIM(RTRIM(CONVERT(varchar(30), a.DATA_EMISSAO, 23))) = '''' THEN CONVERT(varchar(30), a.DATA_EMISSAO) ELSE COALESCE(CONVERT(varchar(10), TRY_CONVERT(date, REPLACE(REPLACE(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), a.DATA_EMISSAO, 23))), 10), ''-'', ''/''), ''.'', ''/''), 103), 23),CONVERT(varchar(10), TRY_CONVERT(date, REPLACE(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), a.DATA_EMISSAO, 23))), 10), ''/'', ''-''), 23), 23),CONVERT(varchar(10), TRY_CONVERT(date, LTRIM(RTRIM(CONVERT(varchar(30), a.DATA_EMISSAO, 23))), 112), 23),CASE WHEN LEN(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), a.DATA_EMISSAO, 23))), 10)) <= 8 THEN CONVERT(varchar(10), TRY_CONVERT(date, REPLACE(REPLACE(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), a.DATA_EMISSAO, 23))), 10), ''-'', ''/''), ''.'', ''/''), 3), 23) END,''1900-01-01'') END
        ,a.DATA_ENTRADA = CASE WHEN a.DATA_ENTRADA IS NULL OR LTRIM(RTRIM(CONVERT(varchar(30), a.DATA_ENTRADA, 23))) = '''' THEN CONVERT(varchar(30), a.DATA_ENTRADA) ELSE COALESCE(CONVERT(varchar(10), TRY_CONVERT(date, REPLACE(REPLACE(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), a.DATA_ENTRADA, 23))), 10), ''-'', ''/''), ''.'', ''/''), 103), 23),CONVERT(varchar(10), TRY_CONVERT(date, REPLACE(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), a.DATA_ENTRADA, 23))), 10), ''/'', ''-''), 23), 23),CONVERT(varchar(10), TRY_CONVERT(date, LTRIM(RTRIM(CONVERT(varchar(30), a.DATA_ENTRADA, 23))), 112), 23),CASE WHEN LEN(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), a.DATA_ENTRADA, 23))), 10)) <= 8 THEN CONVERT(varchar(10), TRY_CONVERT(date, REPLACE(REPLACE(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), a.DATA_ENTRADA, 23))), 10), ''-'', ''/''), ''.'', ''/''), 3), 23) END,''1900-01-01'') END
        ,a.DATA_VENCIMENTO = CASE WHEN a.DATA_VENCIMENTO IS NULL OR LTRIM(RTRIM(CONVERT(varchar(30), a.DATA_VENCIMENTO, 23))) = '''' THEN CONVERT(varchar(30), a.DATA_VENCIMENTO) ELSE COALESCE(CONVERT(varchar(10), TRY_CONVERT(date, REPLACE(REPLACE(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), a.DATA_VENCIMENTO, 23))), 10), ''-'', ''/''), ''.'', ''/''), 103), 23),CONVERT(varchar(10), TRY_CONVERT(date, REPLACE(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), a.DATA_VENCIMENTO, 23))), 10), ''/'', ''-''), 23), 23),CONVERT(varchar(10), TRY_CONVERT(date, LTRIM(RTRIM(CONVERT(varchar(30), a.DATA_VENCIMENTO, 23))), 112), 23),CASE WHEN LEN(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), a.DATA_VENCIMENTO, 23))), 10)) <= 8 THEN CONVERT(varchar(10), TRY_CONVERT(date, REPLACE(REPLACE(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), a.DATA_VENCIMENTO, 23))), 10), ''-'', ''/''), ''.'', ''/''), 3), 23) END,''1900-01-01'') END
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
		a.DT_ANIVER = COALESCE(CONVERT(varchar(10), TRY_CONVERT(date, REPLACE(REPLACE(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), a.DT_ANIVER, 23))), 10), ''-'', ''/''), ''.'', ''/''), 103), 23),CONVERT(varchar(10), TRY_CONVERT(date, REPLACE(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), a.DT_ANIVER, 23))), 10), ''/'', ''-''), 23), 23),CONVERT(varchar(10), TRY_CONVERT(date, LTRIM(RTRIM(CONVERT(varchar(30), a.DT_ANIVER, 23))), 112), 23),CASE WHEN LEN(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), a.DT_ANIVER, 23))), 10)) <= 8 THEN CONVERT(varchar(10), TRY_CONVERT(date, REPLACE(REPLACE(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), a.DT_ANIVER, 23))), 10), ''-'', ''/''), ''.'', ''/''), 3), 23) END,''1900-01-01'')
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
		a.DATA_CADASTRO = CASE WHEN a.DATA_CADASTRO IS NULL OR LTRIM(RTRIM(CONVERT(varchar(30), a.DATA_CADASTRO, 23))) = '''' THEN CONVERT(varchar(10), CAST(GETDATE() AS date), 23) ELSE COALESCE(CONVERT(varchar(10), TRY_CONVERT(date, REPLACE(REPLACE(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), a.DATA_CADASTRO, 23))), 10), ''-'', ''/''), ''.'', ''/''), 103), 23),CONVERT(varchar(10), TRY_CONVERT(date, REPLACE(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), a.DATA_CADASTRO, 23))), 10), ''/'', ''-''), 23), 23),CONVERT(varchar(10), TRY_CONVERT(date, LTRIM(RTRIM(CONVERT(varchar(30), a.DATA_CADASTRO, 23))), 112), 23),CASE WHEN LEN(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), a.DATA_CADASTRO, 23))), 10)) <= 8 THEN CONVERT(varchar(10), TRY_CONVERT(date, REPLACE(REPLACE(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), a.DATA_CADASTRO, 23))), 10), ''-'', ''/''), ''.'', ''/''), 3), 23) END,''1900-01-01'') END
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
		a.LIM_CREDITO_VALIDADE = COALESCE(CONVERT(varchar(10), TRY_CONVERT(date, REPLACE(REPLACE(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), a.LIM_CREDITO_VALIDADE, 23))), 10), ''-'', ''/''), ''.'', ''/''), 103), 23),CONVERT(varchar(10), TRY_CONVERT(date, REPLACE(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), a.LIM_CREDITO_VALIDADE, 23))), 10), ''/'', ''-''), 23), 23),CONVERT(varchar(10), TRY_CONVERT(date, LTRIM(RTRIM(CONVERT(varchar(30), a.LIM_CREDITO_VALIDADE, 23))), 112), 23),CASE WHEN LEN(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), a.LIM_CREDITO_VALIDADE, 23))), 10)) <= 8 THEN CONVERT(varchar(10), TRY_CONVERT(date, REPLACE(REPLACE(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), a.LIM_CREDITO_VALIDADE, 23))), 10), ''-'', ''/''), ''.'', ''/''), 3), 23) END,''1900-01-01'')
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
		a.DATA_CADASTRO = CASE WHEN b.DATA_CADASTRO IS NULL OR LTRIM(RTRIM(CONVERT(varchar(30), b.DATA_CADASTRO, 23))) = '''' THEN CONVERT(varchar(10), CAST(GETDATE() AS date), 23) ELSE COALESCE(CONVERT(varchar(10), TRY_CONVERT(date, REPLACE(REPLACE(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), b.DATA_CADASTRO, 23))), 10), ''-'', ''/''), ''.'', ''/''), 103), 23),CONVERT(varchar(10), TRY_CONVERT(date, REPLACE(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), b.DATA_CADASTRO, 23))), 10), ''/'', ''-''), 23), 23),CONVERT(varchar(10), TRY_CONVERT(date, LTRIM(RTRIM(CONVERT(varchar(30), b.DATA_CADASTRO, 23))), 112), 23),CASE WHEN LEN(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), b.DATA_CADASTRO, 23))), 10)) <= 8 THEN CONVERT(varchar(10), TRY_CONVERT(date, REPLACE(REPLACE(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), b.DATA_CADASTRO, 23))), 10), ''-'', ''/''), ''.'', ''/''), 3), 23) END,''1900-01-01'') END
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
    SET a.DATA_ABERTURA = COALESCE(CONVERT(varchar(10), TRY_CONVERT(date, REPLACE(REPLACE(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), a.DATA_ABERTURA, 23))), 10), ''-'', ''/''), ''.'', ''/''), 103), 23),CONVERT(varchar(10), TRY_CONVERT(date, REPLACE(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), a.DATA_ABERTURA, 23))), 10), ''/'', ''-''), 23), 23),CONVERT(varchar(10), TRY_CONVERT(date, LTRIM(RTRIM(CONVERT(varchar(30), a.DATA_ABERTURA, 23))), 112), 23),CASE WHEN LEN(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), a.DATA_ABERTURA, 23))), 10)) <= 8 THEN CONVERT(varchar(10), TRY_CONVERT(date, REPLACE(REPLACE(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), a.DATA_ABERTURA, 23))), 10), ''-'', ''/''), ''.'', ''/''), 3), 23) END,''1900-01-01'')
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG a
    WHERE a.Flag = 1

    UPDATE a
    SET a.DATA_LIBERACAO = COALESCE(CONVERT(varchar(10), TRY_CONVERT(date, REPLACE(REPLACE(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), a.DATA_LIBERACAO, 23))), 10), ''-'', ''/''), ''.'', ''/''), 103), 23),CONVERT(varchar(10), TRY_CONVERT(date, REPLACE(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), a.DATA_LIBERACAO, 23))), 10), ''/'', ''-''), 23), 23),CONVERT(varchar(10), TRY_CONVERT(date, LTRIM(RTRIM(CONVERT(varchar(30), a.DATA_LIBERACAO, 23))), 112), 23),CASE WHEN LEN(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), a.DATA_LIBERACAO, 23))), 10)) <= 8 THEN CONVERT(varchar(10), TRY_CONVERT(date, REPLACE(REPLACE(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), a.DATA_LIBERACAO, 23))), 10), ''-'', ''/''), ''.'', ''/''), 3), 23) END,''1900-01-01'')
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
    SET a.DATA_MOVIMENTO = COALESCE(CONVERT(varchar(10), TRY_CONVERT(date, REPLACE(REPLACE(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), a.DATA_MOVIMENTO, 23))), 10), ''-'', ''/''), ''.'', ''/''), 103), 23),CONVERT(varchar(10), TRY_CONVERT(date, REPLACE(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), a.DATA_MOVIMENTO, 23))), 10), ''/'', ''-''), 23), 23),CONVERT(varchar(10), TRY_CONVERT(date, LTRIM(RTRIM(CONVERT(varchar(30), a.DATA_MOVIMENTO, 23))), 112), 23),CASE WHEN LEN(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), a.DATA_MOVIMENTO, 23))), 10)) <= 8 THEN CONVERT(varchar(10), TRY_CONVERT(date, REPLACE(REPLACE(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), a.DATA_MOVIMENTO, 23))), 10), ''-'', ''/''), ''.'', ''/''), 3), 23) END,''1900-01-01'')
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.MovimentoEstoque_MG a

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
    SET a.DATA_VENDA = COALESCE(CONVERT(varchar(10), TRY_CONVERT(date, REPLACE(REPLACE(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), a.DATA_VENDA, 23))), 10), ''-'', ''/''), ''.'', ''/''), 103), 23),CONVERT(varchar(10), TRY_CONVERT(date, REPLACE(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), a.DATA_VENDA, 23))), 10), ''/'', ''-''), 23), 23),CONVERT(varchar(10), TRY_CONVERT(date, LTRIM(RTRIM(CONVERT(varchar(30), a.DATA_VENDA, 23))), 112), 23),CASE WHEN LEN(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), a.DATA_VENDA, 23))), 10)) <= 8 THEN CONVERT(varchar(10), TRY_CONVERT(date, REPLACE(REPLACE(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), a.DATA_VENDA, 23))), 10), ''-'', ''/''), ''.'', ''/''), 3), 23) END,''1900-01-01'')
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


-- >>> INICIO: 00_drop_procedures.sql
-- =============================================================================
-- DROP — Procedures De/Para (DadosGX)
-- Pacote DadosGX — gerar drop antes da reinstalacao
-- Gerado em: 01/10/2026 08:20
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Pessoa_DePara_SegmentoMercado' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_01_Pessoa_DePara_SegmentoMercado];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_02_Pessoa_DePara_Escolaridade' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_02_Pessoa_DePara_Escolaridade];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_03_Pessoa_DePara_Profissao' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_03_Pessoa_DePara_Profissao];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_04_Pessoa_DePara_EstadoCivil' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_04_Pessoa_DePara_EstadoCivil];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_05_Pessoa_DePara_Municipio' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_05_Pessoa_DePara_Municipio];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_06_Pessoa_DePara_TipoLogradouro' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_06_Pessoa_DePara_TipoLogradouro];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_07_Pessoa_DePara_Estado_Pais' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_07_Pessoa_DePara_Estado_Pais];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_08_Pessoa_DePara_Banco' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_08_Pessoa_DePara_Banco];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Produto_DePara_Unidade' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_01_Produto_DePara_Unidade];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_02_Produto_DePara_TipoProduto' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_02_Produto_DePara_TipoProduto];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_03_Produto_DePara_GrupoLucratividade' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_03_Produto_DePara_GrupoLucratividade];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_04_Produto_DePara_GrupoProduto' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_04_Produto_DePara_GrupoProduto];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_05_Produto_DePara_Procedencia' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_05_Produto_DePara_Procedencia];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_06_Produto_DePara_TabelaPreco' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_06_Produto_DePara_TabelaPreco];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_ProdutoEstoque_DePara_Estoque' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_01_ProdutoEstoque_DePara_Estoque];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Veiculo_DePara_ModeloVeiculo' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_01_Veiculo_DePara_ModeloVeiculo];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_02_Veiculo_DePara_CorExterna' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_02_Veiculo_DePara_CorExterna];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_03_Veiculo_DePara_CorInterna' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_03_Veiculo_DePara_CorInterna];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_04_Veiculo_DePara_VeiculoAno' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_04_Veiculo_DePara_VeiculoAno];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_05_Veiculo_DePara_Estado' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_05_Veiculo_DePara_Estado];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_06_Veiculo_DePara_Municipio' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_06_Veiculo_DePara_Municipio];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_07_Veiculo_DePara_Marca' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_07_Veiculo_DePara_Marca];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Financeiro_DePara_AgenteCobrador' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_01_Financeiro_DePara_AgenteCobrador];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_02_Financeiro_DePara_ContaGerencial' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_02_Financeiro_DePara_ContaGerencial];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_03_Financeiro_DePara_TipoTitulo' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_03_Financeiro_DePara_TipoTitulo];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_04_Financeiro_DePara_Departamento' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_04_Financeiro_DePara_Departamento];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_05_Financeiro_DePara_NaturezaOperacao' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_05_Financeiro_DePara_NaturezaOperacao];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_06_Financeiro_DePara_Banco' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_06_Financeiro_DePara_Banco];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Adiantamento_DePara_TipoFichaRazao' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_01_Adiantamento_DePara_TipoFichaRazao];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_MovimentoEstoque_DePara_NaturezaOperacao' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_01_MovimentoEstoque_DePara_NaturezaOperacao];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_02_MovimentoEstoque_DePara_Estoque' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_02_MovimentoEstoque_DePara_Estoque];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_03_MovimentoEstoque_DePara_Departamento' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_03_MovimentoEstoque_DePara_Departamento];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Fseg_DePara_TipoOS' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_01_Fseg_DePara_TipoOS];
GO
-- <<< FIM: 00_drop_procedures.sql
GO


-- >>> INICIO: up_01_Pessoa_DePara_SegmentoMercado.sql
-- =============================================================================
-- Layout/trigger: forn_cli (pos-importacao)
-- Destino : SegmentoMercado_DePara
-- Procedure: up_01_Pessoa_DePara_SegmentoMercado
-- Params: @BancoDadosGX, @BancoWF
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Pessoa_DePara_SegmentoMercado' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_01_Pessoa_DePara_SegmentoMercado;
GO

CREATE PROCEDURE dbo.up_01_Pessoa_DePara_SegmentoMercado
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

--TRUNCATE TABLE dbo.SegmentoMercado_DePara

SELECT @CMD='

PRINT ''=========================================================================================='' 
PRINT '' 01 - GERA SegmentoMercado_DePara '' 
PRINT ''==========================================================================================''
	
	INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.SegmentoMercado_DePara
		(segm_cd,
		 segm_ds,
		 SegmentoMercado_Codigo,
		 SegmentoMercado_Descricao)
	
	SELECT DISTINCT
		segm_cd						= '''', 
		segm_ds						= ISNULL(a.SEGMENTO_OFICINA,''''),
		SegmentoMercado_Codigo		= ''S/DePara'',
		SegmentoMercado_Descricao	= ''S/DePara''
	FROM 
		' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_MG a
	WHERE 
		NOT EXISTS (
			SELECT 1 
			FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.SegmentoMercado_DePara c 
			WHERE ISNULL(a.SEGMENTO_OFICINA, '''') = ISNULL(c.segm_ds, '''')
		)
		AND a.Flag = 1

	UNION

	SELECT DISTINCT
		segm_cd						= '''',
		segm_ds						= ISNULL(a.SEGMENTO_BALCAO,''''),
		SegmentoMercado_Codigo		= ''S/DePara'',
		SegmentoMercado_Descricao	= ''S/DePara''
	FROM 
		' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_MG a
	WHERE 
		NOT EXISTS (
			SELECT 1 
			FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.SegmentoMercado_DePara c 
			WHERE ISNULL(a.SEGMENTO_BALCAO, '''') = ISNULL(c.segm_ds, '''')
		)
		AND a.Flag = 1

	UNION

	SELECT DISTINCT
		segm_cd						= '''',
		segm_ds						= ISNULL(a.SEGMENTO_VENDAS,''''),
		SegmentoMercado_Codigo		= ''S/DePara'',
		SegmentoMercado_Descricao	= ''S/DePara''
	FROM 
		' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_MG a
	WHERE 
		NOT EXISTS (
			SELECT 1 
			FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.SegmentoMercado_DePara c 
			WHERE ISNULL(a.SEGMENTO_VENDAS, '''') = ISNULL(c.segm_ds, '''')
		)
		AND a.Flag = 1

'

EXEC sp_executesql @CMD

SELECT @CMD='

PRINT ''=========================================================================================='' 
PRINT '' 02 - Aplica POR DESCRIÇÃO - SegmentoMercado_DePara  '' 
PRINT ''==========================================================================================''

	UPDATE a 
	SET
		a.SegmentoMercado_Codigo		=	b.SegmentoMercado_Codigo,
		a.SegmentoMercado_Descricao		=	b.SegmentoMercado_Descricao
	FROM 
		' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.SegmentoMercado_DePara a
	INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.SegmentoMercado b ON b.SegmentoMercado_Descricao = a.segm_ds
	WHERE
		a.SegmentoMercado_Codigo	= ''S/DePara''

'

EXEC sp_executesql @CMD

GO
-- <<< FIM: up_01_Pessoa_DePara_SegmentoMercado.sql
GO


-- >>> INICIO: up_02_Pessoa_DePara_Escolaridade.sql
-- =============================================================================
-- Layout/trigger: forn_cli (pos-importacao)
-- Destino : Escolaridade_DePara
-- Procedure: up_02_Pessoa_DePara_Escolaridade
-- Params: @BancoDadosGX, @BancoWF
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_02_Pessoa_DePara_Escolaridade' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_02_Pessoa_DePara_Escolaridade;
GO

CREATE PROCEDURE dbo.up_02_Pessoa_DePara_Escolaridade
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

--TRUNCATE TABLE dbo.Escolaridade_DePara

SELECT @CMD='

PRINT ''=========================================================================================='' 
PRINT '' 01 - GERA Escolaridade_DePara '' 
PRINT ''==========================================================================================''
	
	INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Escolaridade_DePara
		(escola_cd,
		 escola_ds,
		 Escolaridade_Codigo,
		 Escolaridade_Descricao)
	SELECT DISTINCT
		escola_cd				= ISNULL(a.ESCOLARIDADE_CODIGO, ''''),
		escola_ds				= ISNULL(a.ESCOLARIDADE_DESCRICAO, ''''),
		Escolaridade_Codigo		= ''S/DePara'',
		Escolaridade_Descricao	= ''S/DePara''
	FROM 
		' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_MG a
	WHERE 
		a.TIPO = ''F'' AND
		a.ESCOLARIDADE_CODIGO IS NOT NULL AND 
		a.Flag = 1 AND
		NOT EXISTS (
			SELECT 1 
			FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Escolaridade_DePara b
			WHERE ISNULL(a.ESCOLARIDADE_CODIGO, '''') = ISNULL(b.escola_cd, '''')
		)

'

EXEC sp_executesql @CMD

SELECT @CMD='

PRINT ''=========================================================================================='' 
PRINT '' 02 - Aplica POR DESCRIÇÃO - Escolaridade_DePara '' 
PRINT ''==========================================================================================''

	UPDATE a 
	SET
		a.Escolaridade_Codigo		=	b.Escolaridade_Codigo,
		a.Escolaridade_Descricao	=	b.Escolaridade_Descricao
	FROM 
		' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Escolaridade_DePara a
	INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Escolaridade b ON b.Escolaridade_Descricao = a.escola_ds
	WHERE
		a.Escolaridade_Codigo	= ''S/DePara''

'

EXEC sp_executesql @CMD

GO
-- <<< FIM: up_02_Pessoa_DePara_Escolaridade.sql
GO


-- >>> INICIO: up_03_Pessoa_DePara_Profissao.sql
-- =============================================================================
-- Layout/trigger: forn_cli (pos-importacao)
-- Destino : Profissao_DePara
-- Procedure: up_03_Pessoa_DePara_Profissao
-- Params: @BancoDadosGX, @BancoWF
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_03_Pessoa_DePara_Profissao' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_03_Pessoa_DePara_Profissao;
GO

CREATE PROCEDURE dbo.up_03_Pessoa_DePara_Profissao
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

--TRUNCATE TABLE dbo.Profissao_DePara

SELECT @CMD='

PRINT ''=========================================================================================='' 
PRINT '' 01 - GERA Profissao_DePara '' 
PRINT ''==========================================================================================''
	
	INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Profissao_DePara
		(prof_cd,
		 prof_ds,
		 Profissao_Codigo,
		 Profissao_Descricao)
	SELECT DISTINCT
		prof_cd				= ISNULL(a.PROFISSAO_CODIGO, ''''),
		prof_ds				= ISNULL(a.PROFISSAO_DESCRICAO, ''''),
		Profissao_Codigo	= ''S/DePara'',
		Profissao_Descricao	= ''S/DePara''
	FROM 
		' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_MG a
	WHERE 
		a.TIPO = ''F'' AND
		a.PROFISSAO_CODIGO IS NOT NULL AND 
		a.Flag = 1 AND
		NOT EXISTS (
			SELECT 1 
			FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Profissao_DePara b
			WHERE ISNULL(a.PROFISSAO_CODIGO, '''') = ISNULL(b.prof_cd, '''')
		)

'

EXEC sp_executesql @CMD

SELECT @CMD='

PRINT ''=========================================================================================='' 
PRINT '' 02 - Aplica POR DESCRIÇÃO - Profissao_DePara '' 
PRINT ''==========================================================================================''

	UPDATE a 
	SET
		a.Profissao_Codigo		=	b.Profissao_Codigo,
		a.Profissao_Descricao	=	b.Profissao_Descricao
	FROM 
		' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Profissao_DePara a
	INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Profissao b ON b.Profissao_Descricao = a.prof_ds COLLATE Latin1_General_CI_AI
	WHERE
		a.Profissao_Codigo	= ''S/DePara''

'

EXEC sp_executesql @CMD

GO
-- <<< FIM: up_03_Pessoa_DePara_Profissao.sql
GO


-- >>> INICIO: up_04_Pessoa_DePara_EstadoCivil.sql
-- =============================================================================
-- Layout/trigger: forn_cli (pos-importacao)
-- Destino : EstadoCivil_DePara
-- Procedure: up_04_Pessoa_DePara_EstadoCivil
-- Params: @BancoDadosGX, @BancoWF
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_04_Pessoa_DePara_EstadoCivil' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_04_Pessoa_DePara_EstadoCivil;
GO

CREATE PROCEDURE dbo.up_04_Pessoa_DePara_EstadoCivil
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

--TRUNCATE TABLE dbo.EstadoCivil_DePara

SELECT @CMD='

PRINT ''=========================================================================================='' 
PRINT '' 01 - GERA EstadoCivil_DePara '' 
PRINT ''==========================================================================================''
	
	INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.EstadoCivil_DePara
		(estcivil_cd,
		 estcivil_ds,
		 EstadoCivil_Codigo,
		 EstadoCivil_Descricao)
	SELECT DISTINCT
		estcivil_cd            = ISNULL(a.ESTADO_CIVIL_CODIGO, ''''),
		estcivil_ds            = ISNULL(a.ESTADO_CIVIL_DESCRICAO, ''''),
		EstadoCivil_Codigo     = ''S/DePara'',
		EstadoCivil_Descricao  = ''S/DePara''
	FROM 
		' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_MG a
	WHERE 
		a.TIPO = ''F'' AND
		a.ESTADO_CIVIL_CODIGO IS NOT NULL AND 
		a.Flag = 1 AND
		NOT EXISTS (
			SELECT 1 
			FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.EstadoCivil_DePara b
			WHERE ISNULL(a.ESTADO_CIVIL_CODIGO, '''') = ISNULL(b.estcivil_cd, '''')
		)

'

EXEC sp_executesql @CMD

SELECT @CMD='

PRINT ''=========================================================================================='' 
PRINT '' 02 - Aplica POR DESCRIÇÃO - EstadoCivil_DePara '' 
PRINT ''==========================================================================================''

	UPDATE a 
	SET
		a.EstadoCivil_Codigo = b.EstadoCivil_Codigo,
		a.EstadoCivil_Descricao = b.EstadoCivil_Descricao
	FROM 
		' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.EstadoCivil_DePara a
	INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.EstadoCivil b ON REPLACE(b.EstadoCivil_Descricao,''(A)'','''') = a.estcivil_ds COLLATE Latin1_General_CI_AI
	WHERE
		a.EstadoCivil_Codigo	= ''S/DePara''
'

EXEC sp_executesql @CMD

GO
-- <<< FIM: up_04_Pessoa_DePara_EstadoCivil.sql
GO


-- >>> INICIO: up_05_Pessoa_DePara_Municipio.sql
-- =============================================================================
-- Layout/trigger: forn_cli_endereco (pos-importacao)
-- Destino : Municipio_DePara
-- Procedure: up_05_Pessoa_DePara_Municipio
-- Params: @BancoDadosGX, @BancoWF
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_05_Pessoa_DePara_Municipio' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_05_Pessoa_DePara_Municipio;
GO

CREATE PROCEDURE dbo.up_05_Pessoa_DePara_Municipio
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

--TRUNCATE TABLE dbo.Municipio_DePara

SELECT @CMD='

PRINT ''=========================================================================================='' 
PRINT '' 01 - GERA Municipio_DePara '' 
PRINT ''==========================================================================================''
	
	INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Municipio_DePara
		(cg_cidade
		,Municipio_IBGE
		,uf_cd
		,Municipio_Codigo
		,Municipio_Nome
		,Estado_Codigo
		,Tabela)
	SELECT DISTINCT
		cg_cidade			= ISNULL(RTRIM(LTRIM(UPPER(a.CIDADE))) COLLATE SQL_Latin1_General_CP1253_CI_AI,'''')
		,Municipio_IBGE		= ISNULL(RTRIM(LTRIM(a.COD_IBGE)),'''')
		,uf_cd				= ISNULL(RTRIM(LTRIM(a.ESTADO)),'''')
		,Municipio_Codigo	= ''S/DePara''
		,Municipio_Nome		= ''S/DePara''
		,Estado_Codigo		= ''S/DePara''
		,Tabela				= ''Pessoa''
	FROM 
		' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaEndereco_MG a
	WHERE 
		a.Flag = 1 AND
		NOT EXISTS (
			SELECT 1 
			FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Municipio_DePara b
			WHERE
				RTRIM( LTRIM( ISNULL(a.CIDADE,'''' ) ) )	= RTRIM( LTRIM( ISNULL(b.cg_cidade,'''' ) ) )	COLLATE SQL_Latin1_General_CP1253_CI_AI
				AND	RTRIM( LTRIM( ISNULL(a.ESTADO,'''' ) ) )	= ISNULL(b.uf_cd,'''')						COLLATE SQL_Latin1_General_CP1253_CI_AI
				AND	RTRIM( LTRIM( ISNULL(a.COD_IBGE,'''' ) ) )	= ISNULL(b.Municipio_IBGE,'''')				COLLATE SQL_Latin1_General_CP1253_CI_AI
		)
'

EXEC sp_executesql @CMD

SELECT @CMD='

PRINT ''=========================================================================================='' 
PRINT '' 02 - Aplica POR COD_IBGE/cg_cidade/uf_cd - Municipio_DePara '' 
PRINT ''==========================================================================================''
	
	UPDATE a 
	SET
		a.Municipio_Codigo	= b.Municipio_Codigo
		,a.Municipio_Nome	= b.Municipio_Nome
		,a.Estado_Codigo	= b.Estado_Codigo
	FROM 
		'+LTRIM(RTRIM(@BancoDadosGX))+'.dbo.Municipio_DePara a
	INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Municipio b ON a.Municipio_IBGE	=	RTRIM( LTRIM(b.Municipio_IBGE) )	COLLATE SQL_Latin1_General_CP1253_CI_AI 
													AND	RTRIM( LTRIM(a.cg_cidade) )	=	RTRIM( LTRIM(b.Municipio_Nome) )	COLLATE SQL_Latin1_General_CP1253_CI_AI
	     											AND	RTRIM( LTRIM(a.uf_cd) )		=	b.Estado_Codigo						COLLATE SQL_Latin1_General_CP1253_CI_AI
	WHERE
		ISNUMERIC(a.Municipio_IBGE) = 1		AND 
		ISNUMERIC(a.Municipio_Codigo) = 0	

'

EXEC sp_executesql @CMD

SELECT @CMD='

PRINT ''=========================================================================================='' 
PRINT '' 03 - Aplica POR COD_IBGE/uf_cd - Municipio_DePara '' 
PRINT ''==========================================================================================''
	
	UPDATE a 
	SET
		a.Municipio_Codigo	= b.Municipio_Codigo
		,a.Municipio_Nome	= b.Municipio_Nome
		,a.Estado_Codigo	= b.Estado_Codigo
	FROM 
		'+LTRIM(RTRIM(@BancoDadosGX))+'.dbo.Municipio_DePara a
	INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Municipio b ON a.Municipio_IBGE	=	RTRIM( LTRIM(b.Municipio_IBGE) )	COLLATE SQL_Latin1_General_CP1253_CI_AI 
	     											AND	RTRIM( LTRIM(a.uf_cd) )		=	b.Estado_Codigo						COLLATE SQL_Latin1_General_CP1253_CI_AI
	WHERE
		ISNUMERIC(a.Municipio_IBGE) = 1		AND 
		ISNUMERIC(a.Municipio_Codigo) = 0	

'

EXEC sp_executesql @CMD

SELECT @CMD='

PRINT ''=========================================================================================='' 
PRINT '' 04 - Aplica POR COD_IBGE - Municipio_DePara '' 
PRINT ''==========================================================================================''
	
	UPDATE a 
	SET
		a.Municipio_Codigo	= b.Municipio_Codigo
		,a.Municipio_Nome	= b.Municipio_Nome
		,a.Estado_Codigo	= b.Estado_Codigo
	FROM 
		'+LTRIM(RTRIM(@BancoDadosGX))+'.dbo.Municipio_DePara a
	INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Municipio b ON a.Municipio_IBGE	=	RTRIM( LTRIM(b.Municipio_IBGE) )	COLLATE SQL_Latin1_General_CP1253_CI_AI 
	WHERE
		ISNUMERIC(a.Municipio_IBGE) = 1		AND 
		ISNUMERIC(a.Municipio_Codigo) = 0	

'

EXEC sp_executesql @CMD

SELECT @CMD='

PRINT ''=========================================================================================='' 
PRINT '' 04 - Aplica POR cg_cidade/uf_cd - Municipio_DePara '' 
PRINT ''==========================================================================================''
	
	UPDATE a 
	SET
		a.Municipio_Codigo	= b.Municipio_Codigo
		,a.Municipio_Nome	= b.Municipio_Nome
		,a.Estado_Codigo	= b.Estado_Codigo
	FROM 
		'+LTRIM(RTRIM(@BancoDadosGX))+'.dbo.Municipio_DePara a
	INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Municipio b ON RTRIM( LTRIM(a.cg_cidade) )	=	RTRIM( LTRIM(b.Municipio_Nome) )	COLLATE SQL_Latin1_General_CP1253_CI_AI
	     														AND	RTRIM( LTRIM(a.uf_cd) )		=	b.Estado_Codigo						COLLATE SQL_Latin1_General_CP1253_CI_AI
	WHERE
		ISNUMERIC(a.Municipio_Codigo) = 0	

'

EXEC sp_executesql @CMD

GO
-- <<< FIM: up_05_Pessoa_DePara_Municipio.sql
GO


-- >>> INICIO: up_06_Pessoa_DePara_TipoLogradouro.sql
-- =============================================================================
-- Layout/trigger: forn_cli_endereco (pos-importacao)
-- Destino : TipoLogradouro_DePara
-- Procedure: up_06_Pessoa_DePara_TipoLogradouro
-- Params: @BancoDadosGX, @BancoWF
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_06_Pessoa_DePara_TipoLogradouro' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_06_Pessoa_DePara_TipoLogradouro;
GO

CREATE PROCEDURE dbo.up_06_Pessoa_DePara_TipoLogradouro
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

--TRUNCATE TABLE dbo.TipoLogradouro_DePara

SELECT @CMD='

PRINT ''=========================================================================================='' 
PRINT '' 01 - GERA TipoLogradouro_DePara '' 
PRINT ''==========================================================================================''
	
	INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoLogradouro_DePara
		(logradouro_sigla
		,logradouro_nm
		,TipoLogradouro_Codigo
		,TipoLogradouro_Sigla
		,TipoLogradouro_Descricao
		,Tabela)
	SELECT DISTINCT
		logradouro_sigla			= ''''
		,logradouro_nm				= ISNULL( RTRIM( LTRIM(a.TIPO_LOGRADOURO)),'''')
		,TipoLogradouro_Codigo		= ''S/DePara''
		,TipoLogradouro_Sigla		= ''S/DePara''
		,TipoLogradouro_Descricao	= ''S/DePara''
		,Tabela						= ''Pessoa''
	FROM 
		' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaEndereco_MG a
	WHERE 
		a.Flag = 1 AND
		NOT EXISTS (
			SELECT 1 
			FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoLogradouro_DePara b
			WHERE
				ISNULL(a.TIPO_LOGRADOURO,'''') = ISNULL(b.logradouro_nm,'''') COLLATE Latin1_General_CI_AI
		) 

'

EXEC sp_executesql @CMD

SELECT @CMD='

PRINT ''=========================================================================================='' 
PRINT '' 02 - Aplica POR DESCRIÇÃO - TipoLogradouro_DePara '' 
PRINT ''==========================================================================================''

	UPDATE a 
	SET
		a.TipoLogradouro_Codigo = b.TipoLogradouro_Codigo,
		a.TipoLogradouro_Sigla	= b.TipoLogradouro_Sigla,
		a.TipoLogradouro_Descricao = b.TipoLogradouro_Descricao
	FROM 
		' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoLogradouro_DePara a
	INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.TipoLogradouro b ON b.TipoLogradouro_Descricao = a.logradouro_nm
	WHERE
		a.TipoLogradouro_Codigo	= ''S/DePara''
'

EXEC sp_executesql @CMD

GO
-- <<< FIM: up_06_Pessoa_DePara_TipoLogradouro.sql
GO


-- >>> INICIO: up_07_Pessoa_DePara_Estado_Pais.sql
-- =============================================================================
-- Layout/trigger: forn_cli_endereco (pos-importacao)
-- Destino : Estado_DePara, Pais_DePara
-- Procedure: up_07_Pessoa_DePara_Estado_Pais
-- Params: @BancoDadosGX, @BancoWF
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================
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
-- <<< FIM: up_07_Pessoa_DePara_Estado_Pais.sql
GO


-- >>> INICIO: up_08_Pessoa_DePara_Banco.sql
-- =============================================================================
-- Layout/trigger: forn_cli_dados_bancarios (pos-importacao)
-- Destino : Banco_DePara
-- Procedure: up_08_Pessoa_DePara_Banco
-- Params: @BancoDadosGX, @BancoWF
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================
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
-- <<< FIM: up_08_Pessoa_DePara_Banco.sql
GO


-- >>> INICIO: up_01_Produto_DePara_Unidade.sql
-- =============================================================================
-- Layout: 7 Produto De/Para
-- Tabela : Unidade_DePara
-- Procedure: up_01_Produto_DePara_Unidade (@BancoDadosGX, @BancoWF)
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Produto_DePara_Unidade' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_01_Produto_DePara_Unidade;
GO

CREATE PROCEDURE dbo.up_01_Produto_DePara_Unidade
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF (NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX))
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

SELECT @CMD = '

PRINT ''=========================================================================================='' 
PRINT '' 01 - GERA Unidade_DePara '' 
PRINT ''==========================================================================================''

    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Unidade_DePara
        (pdund_cd, pdund_ds, Unidade_Codigo, Unidade_Descricao)
    SELECT DISTINCT
        pdund_cd = ISNULL(a.UNIDADE_PRODUTO_CODIGO, ''''),
        pdund_ds = ISNULL(a.UNIDADE_PRODUTO_DESCRICAO, ''''),
        Unidade_Codigo = ''S/DePara'',
        Unidade_Descricao = ''S/DePara''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG a
    WHERE
        a.Flag = 1 AND
        NOT EXISTS (
            SELECT 1
            FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Unidade_DePara b
            WHERE ISNULL(a.UNIDADE_PRODUTO_CODIGO, '''') = ISNULL(b.pdund_cd, '''')
        )
'
EXEC sp_executesql @CMD

SELECT @CMD = '

PRINT ''=========================================================================================='' 
PRINT '' 02 - Aplica POR DESCRICAO - Unidade_DePara '' 
PRINT ''==========================================================================================''

    UPDATE a
    SET
        a.Unidade_Codigo = b.Unidade_Codigo,
        a.Unidade_Descricao = b.Unidade_Descricao
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Unidade_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Unidade b ON b.Unidade_Descricao = a.pdund_ds COLLATE Latin1_General_CI_AI
    WHERE
        a.Unidade_Codigo = ''S/DePara''
'
EXEC sp_executesql @CMD
GO
-- <<< FIM: up_01_Produto_DePara_Unidade.sql
GO


-- >>> INICIO: up_02_Produto_DePara_TipoProduto.sql
-- =============================================================================
-- Layout: 7 Produto De/Para
-- Tabela : TipoProduto_DePara
-- Procedure: up_02_Produto_DePara_TipoProduto (@BancoDadosGX, @BancoWF)
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_02_Produto_DePara_TipoProduto' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_02_Produto_DePara_TipoProduto;
GO

CREATE PROCEDURE dbo.up_02_Produto_DePara_TipoProduto
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF (NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX))
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

SELECT @CMD = '

PRINT ''=========================================================================================='' 
PRINT '' 01 - GERA TipoProduto_DePara '' 
PRINT ''==========================================================================================''

    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoProduto_DePara
        (tpd_cd, tpd_ds, TipoProduto_Codigo, TipoProduto_Descricao, TipoProduto_GrupoContabilCod)
    SELECT DISTINCT
        tpd_cd = ISNULL(a.TIPO_PRODUTO_CODIGO, ''''),
        tpd_ds = ISNULL(a.TIPO_PRODUTO_DESCRICAO, ''''),
        TipoProduto_Codigo = ''S/DePara'',
        TipoProduto_Descricao = ''S/DePara'',
        TipoProduto_GrupoContabilCod = ''S/DePara''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG a
    WHERE
        a.Flag = 1 AND
        NOT EXISTS (
            SELECT 1
            FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoProduto_DePara b
            WHERE ISNULL(a.TIPO_PRODUTO_CODIGO, '''') = ISNULL(b.tpd_cd, '''')
        )
'
EXEC sp_executesql @CMD

SELECT @CMD = '

PRINT ''=========================================================================================='' 
PRINT '' 02 - Aplica POR DESCRICAO - TipoProduto_DePara '' 
PRINT ''==========================================================================================''

    UPDATE a
    SET
        a.TipoProduto_Codigo = b.TipoProduto_Codigo,
        a.TipoProduto_Descricao = b.TipoProduto_Descricao,
        a.TipoProduto_GrupoContabilCod = b.TipoProduto_GrupoContabilCod
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoProduto_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.TipoProduto b ON b.TipoProduto_Descricao = a.tpd_ds COLLATE Latin1_General_CI_AI
    WHERE
        a.TipoProduto_Codigo = ''S/DePara''
'
EXEC sp_executesql @CMD
GO
-- <<< FIM: up_02_Produto_DePara_TipoProduto.sql
GO


-- >>> INICIO: up_03_Produto_DePara_GrupoLucratividade.sql
-- =============================================================================
-- Layout: 7 Produto De/Para
-- Tabela : GrupoLucratividade_DePara
-- Procedure: up_03_Produto_DePara_GrupoLucratividade (@BancoDadosGX, @BancoWF)
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_03_Produto_DePara_GrupoLucratividade' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_03_Produto_DePara_GrupoLucratividade;
GO

CREATE PROCEDURE dbo.up_03_Produto_DePara_GrupoLucratividade
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF (NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX))
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

SELECT @CMD = '

PRINT ''=========================================================================================='' 
PRINT '' 01 - GERA GrupoLucratividade_DePara '' 
PRINT ''==========================================================================================''

    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.GrupoLucratividade_DePara
        (letr_cd, letr_ds, letr_cdmont, GrupoLucratividade_Codigo, GrupoLucratividade_Descricao, GrupoLucratividade_MarcaCod, GrupoLucratividade_Letra)
    SELECT DISTINCT
        letr_cd = ISNULL(a.GRUPO_LUCRATIVIDADE_CODIGO, ''''),
        letr_ds = ISNULL(a.GRUPO_LUCRATIVIDADE_DESCRICAO, ''''),
        letr_cdmont = '''',
        GrupoLucratividade_Codigo = ''S/DePara'',
        GrupoLucratividade_Descricao = ''S/DePara'',
        GrupoLucratividade_MarcaCod = ISNULL(a.ProdutoMarca_MarcaCod, ''''),
        GrupoLucratividade_Letra = ''S/DePara''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG a
    WHERE
        a.Flag = 1 AND
        NOT EXISTS (
            SELECT 1
            FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.GrupoLucratividade_DePara b
            WHERE
                ISNULL(a.GRUPO_LUCRATIVIDADE_CODIGO, '''') = ISNULL(b.letr_cd, '''') AND
                ISNULL(a.GRUPO_LUCRATIVIDADE_DESCRICAO, '''') = ISNULL(b.letr_ds, '''') AND
                a.ProdutoMarca_MarcaCod = b.GrupoLucratividade_MarcaCod
        )
'
EXEC sp_executesql @CMD

SELECT @CMD = '

PRINT ''=========================================================================================='' 
PRINT '' 02 - Aplica POR DESCRICAO - GrupoLucratividade_DePara '' 
PRINT ''==========================================================================================''

    UPDATE a
    SET
        a.GrupoLucratividade_Codigo = b.GrupoLucratividade_Codigo,
        a.GrupoLucratividade_Descricao = b.GrupoLucratividade_Descricao,
        a.GrupoLucratividade_MarcaCod = b.GrupoLucratividade_MarcaCod,
        a.GrupoLucratividade_Letra = b.GrupoLucratividade_Letra
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.GrupoLucratividade_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.GrupoLucratividade b ON
        b.GrupoLucratividade_Letra = RTRIM(LTRIM(a.letr_cd)) COLLATE Latin1_General_CI_AI AND
        a.GrupoLucratividade_MarcaCod = b.GrupoLucratividade_MarcaCod
    WHERE
        a.GrupoLucratividade_Codigo = ''S/DePara''
'
EXEC sp_executesql @CMD
GO
-- <<< FIM: up_03_Produto_DePara_GrupoLucratividade.sql
GO


-- >>> INICIO: up_04_Produto_DePara_GrupoProduto.sql
-- =============================================================================
-- Layout: 7 Produto De/Para
-- Tabela : GrupoProduto_DePara
-- Procedure: up_04_Produto_DePara_GrupoProduto (@BancoDadosGX, @BancoWF)
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_04_Produto_DePara_GrupoProduto' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_04_Produto_DePara_GrupoProduto;
GO

CREATE PROCEDURE dbo.up_04_Produto_DePara_GrupoProduto
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF (NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX))
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

SELECT @CMD = '

PRINT ''=========================================================================================='' 
PRINT '' 01 - GERA GrupoProduto_DePara '' 
PRINT ''==========================================================================================''

    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.GrupoProduto_DePara
        (grup_cd, grup_ds, GrupoProduto_Codigo, GrupoProduto_Descricao)
    SELECT DISTINCT
        grup_cd = ISNULL(a.GRUPO_PRODUTO_CODIGO, ''''),
        grup_ds = ISNULL(a.GRUPO_PRODUTO_DESCRICAO, ''''),
        GrupoProduto_Codigo = ''S/DePara'',
        GrupoProduto_Descricao = ''S/DePara''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG a
    WHERE
        a.Flag = 1 AND
        NOT EXISTS (
            SELECT 1
            FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.GrupoProduto_DePara b
            WHERE ISNULL(a.GRUPO_PRODUTO_CODIGO, '''') = ISNULL(b.grup_cd, '''')
        )
'
EXEC sp_executesql @CMD

SELECT @CMD = '

PRINT ''=========================================================================================='' 
PRINT '' 02 - Aplica POR DESCRICAO - GrupoProduto_DePara '' 
PRINT ''==========================================================================================''

    UPDATE a
    SET
        a.GrupoProduto_Codigo = b.GrupoProduto_Codigo,
        a.GrupoProduto_Descricao = b.GrupoProduto_Descricao
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.GrupoProduto_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.GrupoProduto b ON b.GrupoProduto_Descricao = a.grup_ds COLLATE Latin1_General_CI_AI
    WHERE
        a.GrupoProduto_Codigo = ''S/DePara''
'
EXEC sp_executesql @CMD
GO
-- <<< FIM: up_04_Produto_DePara_GrupoProduto.sql
GO


-- >>> INICIO: up_05_Produto_DePara_Procedencia.sql
-- =============================================================================
-- Layout: 7 Produto De/Para
-- Tabela : Procedencia_DePara
-- Procedure: up_05_Produto_DePara_Procedencia (@BancoDadosGX, @BancoWF)
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_05_Produto_DePara_Procedencia' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_05_Produto_DePara_Procedencia;
GO

CREATE PROCEDURE dbo.up_05_Produto_DePara_Procedencia
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF (NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX))
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

SELECT @CMD = '

PRINT ''=========================================================================================='' 
PRINT '' 01 - GERA Procedencia_DePara '' 
PRINT ''==========================================================================================''

    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Procedencia_DePara
        (pro_cd, pro_ds, Procedencia_Codigo, Procedencia_Descricao)
    SELECT DISTINCT
        pro_cd = ISNULL(a.PROCEDENCIA_CODIGO, ''''),
        pro_ds = ISNULL(a.PROCEDENCIA_DESCRICAO, ''''),
        Procedencia_Codigo = ''S/DePara'',
        Procedencia_Descricao = ''S/DePara''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG a
    WHERE
        a.Flag = 1 AND
        NOT EXISTS (
            SELECT 1
            FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Procedencia_DePara b
            WHERE ISNULL(a.PROCEDENCIA_CODIGO, '''') = ISNULL(b.pro_cd, '''')
        )
'
EXEC sp_executesql @CMD

SELECT @CMD = '

PRINT ''=========================================================================================='' 
PRINT '' 02 - Aplica POR DESCRICAO - Procedencia_DePara '' 
PRINT ''==========================================================================================''

    UPDATE a
    SET
        a.Procedencia_Codigo = b.Procedencia_Codigo,
        a.Procedencia_Descricao = b.Procedencia_Descricao
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Procedencia_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Procedencia b ON b.Procedencia_Descricao = a.pro_ds COLLATE Latin1_General_CI_AI
    WHERE
        a.Procedencia_Codigo = ''S/DePara''
'
EXEC sp_executesql @CMD
GO
-- <<< FIM: up_05_Produto_DePara_Procedencia.sql
GO


-- >>> INICIO: up_06_Produto_DePara_TabelaPreco.sql
-- =============================================================================
-- Layout: 7 Produto De/Para
-- Tabela : TabelaPreco_DePara
-- Procedure: up_06_Produto_DePara_TabelaPreco (@BancoDadosGX, @BancoWF)
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_06_Produto_DePara_TabelaPreco' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_06_Produto_DePara_TabelaPreco;
GO

CREATE PROCEDURE dbo.up_06_Produto_DePara_TabelaPreco
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF (NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX))
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

SELECT @CMD = '

PRINT ''=========================================================================================='' 
PRINT '' 01 - GERA TabelaPreco_DePara '' 
PRINT ''==========================================================================================''

    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TabelaPreco_DePara
        (Empresa_Codigo, Empresa_NomeFantasia, EmpresaTabelaPreco_TabPrecoCod, EmpresaTabelaPreco_TabelaPrecoTipo, TabelaPreco_Codigo, TabelaPreco_Descricao, TabelaPreco_Tipo, banco_principal)
    SELECT DISTINCT
        Empresa_Codigo = a.Empresa_Codigo,
        Empresa_NomeFantasia = b.Empresa_NomeFantasia,
        EmpresaTabelaPreco_TabPrecoCod = a.EmpresaTabelaPreco_TabPrecoCod,
        EmpresaTabelaPreco_TabelaPrecoTipo = a.EmpresaTabelaPreco_TabelaPrecoTipo,
        TabelaPreco_Codigo = c.TabelaPreco_Codigo,
        TabelaPreco_Descricao = c.TabelaPreco_Descricao,
        TabelaPreco_Tipo = c.TabelaPreco_Tipo,
        banco_principal = b.Empresa_MarcaCod
    FROM ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.EmpresaTabelaPreco a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Empresa b ON (a.Empresa_Codigo = b.Empresa_Codigo)
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.TabelaPreco c ON (
        a.EmpresaTabelaPreco_TabPrecoCod = c.TabelaPreco_Codigo AND
        a.EmpresaTabelaPreco_TabelaPrecoTipo = c.TabelaPreco_Tipo
    )
    INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Empresa_DePara d ON (b.Empresa_Codigo = d.Empresa_Codigo)
    WHERE
        NOT EXISTS (
            SELECT 1
            FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TabelaPreco_DePara e
            WHERE
                e.Empresa_Codigo = a.Empresa_Codigo AND
                e.EmpresaTabelaPreco_TabPrecoCod = a.EmpresaTabelaPreco_TabPrecoCod
        )
'
EXEC sp_executesql @CMD
GO
-- <<< FIM: up_06_Produto_DePara_TabelaPreco.sql
GO


-- >>> INICIO: up_01_ProdutoEstoque_DePara_Estoque.sql
-- =============================================================================
-- Layout: 8 ProdutoEstoque De/Para
-- Tabela : Estoque_DePara
-- Procedure: up_01_ProdutoEstoque_DePara_Estoque (@BancoDadosGX, @BancoWF)
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_ProdutoEstoque_DePara_Estoque' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_01_ProdutoEstoque_DePara_Estoque;
GO

CREATE PROCEDURE dbo.up_01_ProdutoEstoque_DePara_Estoque
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF (NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX))
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

IF (NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoWF))
BEGIN
    PRINT 'O < ' + @BancoWF + ' > INFORMADO COMO @BancoWF NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

SELECT @CMD = '
PRINT ''==========================================================================================''
PRINT '' 01 - GERA Estoque_DePara (origem ProdutoEstoque_MG)''
PRINT ''==========================================================================================''

    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Estoque_DePara
        (est_cd, est_ds)
    SELECT DISTINCT
        est_cd = RTRIM(LTRIM(a.ESTOQUE_CODIGO)),
        est_ds = RTRIM(LTRIM(a.ESTOQUE_CODIGO))
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ProdutoEstoque_MG a
    WHERE
        a.Flag = 1
        AND RTRIM(LTRIM(ISNULL(a.ESTOQUE_CODIGO, ''''))) <> ''''
        AND NOT EXISTS (
            SELECT 1
            FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Estoque_DePara b
            WHERE b.est_cd = RTRIM(LTRIM(a.ESTOQUE_CODIGO))
        )

PRINT ''==========================================================================================''
PRINT '' 02 - ATUALIZA Estoque_Codigo / Estoque_Descricao via WF''
PRINT ''==========================================================================================''

    UPDATE a
    SET
        a.Estoque_Codigo = b.Estoque_Codigo,
        a.Estoque_Descricao = b.Estoque_Descricao,
        a.Origem = ''ProdutoEstoque''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Estoque_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Estoque b
        ON RTRIM(LTRIM(a.est_cd)) = RTRIM(LTRIM(b.Estoque_Sigla)) COLLATE database_default
'
EXEC sp_executesql @CMD
GO
-- <<< FIM: up_01_ProdutoEstoque_DePara_Estoque.sql
GO


-- >>> INICIO: up_01_Veiculo_DePara_ModeloVeiculo.sql
-- =============================================================================
-- Layout: Veiculo De/Para — ModeloVeiculo
-- Procedure: up_01_Veiculo_DePara_ModeloVeiculo (@BancoDadosGX, @BancoWF)
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Veiculo_DePara_ModeloVeiculo' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_01_Veiculo_DePara_ModeloVeiculo;
GO

CREATE PROCEDURE dbo.up_01_Veiculo_DePara_ModeloVeiculo
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX)
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

-- 1) Garante colunas extras em ModeloVeiculo_DePara em lote PRÓPRIO.
--    (ALTER ADD e o uso da coluna não podem estar no mesmo lote: o SQL Server
--     compila o lote inteiro antes de executar e acusaria "Invalid column name".)
SELECT @CMD = '
    IF NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns WHERE name = ''MARCA_CODIGO'' AND object_id = OBJECT_ID(''' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ModeloVeiculo_DePara''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ModeloVeiculo_DePara ADD MARCA_CODIGO nvarchar(510) NULL
'
EXEC sp_executesql @CMD

SELECT @CMD = '
    IF NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns WHERE name = ''CODIGO_LINHA'' AND object_id = OBJECT_ID(''' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ModeloVeiculo_DePara''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ModeloVeiculo_DePara ADD CODIGO_LINHA nvarchar(510) NULL
'
EXEC sp_executesql @CMD

-- 2) Atualiza ModeloVeiculoWF (VOLKS/MAN) — lote próprio.
SELECT @CMD = '
    UPDATE a
    SET a.ModeloVeiculoWF = m.ModeloVeiculo_Codigo
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
    LEFT JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.ModeloVeiculo m ON (
        RTRIM(LTRIM(m.ModeloVeiculo_ModeloMarca)) = RTRIM(LTRIM(a.CODIGO_LINHA)) COLLATE Latin1_General_CI_AI
        AND RTRIM(LTRIM(a.CODIGO_LINHA)) <> ''''
        AND a.Marca_CodigoWF = m.ModeloVeiculo_MarcaCod
    )
    WHERE a.Marca_CodigoWF IN (36, 54)
'
EXEC sp_executesql @CMD

-- 3) Gera ModeloVeiculo_DePara — lote próprio (colunas já existem).
SELECT @CMD = '
    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ModeloVeiculo_DePara
        (mod_cd, mod_ds, ModeloVeiculo_Codigo, ModeloVeiculo_Descricao, ModeloVeiculo_MarcaCod,
         ModeloVeiculo_ModeloMarca, molicar_cd, ModeloVeiculo_TabelaMolicar, MARCA_CODIGO, CODIGO_LINHA)
    SELECT DISTINCT
        mod_cd = ISNULL(a.MODELO_CODIGO, ''''),
        mod_ds = ISNULL(RTRIM(LTRIM(a.MODELO_DESCRICAO)), ''''),
        ModeloVeiculo_Codigo = ''S/DePara'',
        ModeloVeiculo_Descricao = ''S/DePara'',
        ModeloVeiculo_MarcaCod = ISNULL(CAST(a.Marca_CodigoWF AS varchar), ''S/DePara''),
        ModeloVeiculo_ModeloMarca = ''S/DePara'',
        molicar_cd = '''',
        ModeloVeiculo_TabelaMolicar = '''',
        MARCA_CODIGO = ISNULL(a.VEICULO_MARCA_CODIGO, ''''),
        CODIGO_LINHA = ISNULL(a.CODIGO_LINHA, '''')
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
    WHERE a.Flag = 1
        AND a.ModeloVeiculoWF IS NULL
        AND NOT EXISTS (
            SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ModeloVeiculo_DePara b
            WHERE ISNULL(a.VEICULO_MARCA_CODIGO, '''') = b.MARCA_CODIGO COLLATE database_default
              AND ISNULL(a.MODELO_CODIGO, '''') = b.mod_cd COLLATE database_default
              AND ISNULL(a.CODIGO_LINHA, '''') = b.CODIGO_LINHA COLLATE database_default
        )
'
EXEC sp_executesql @CMD

-- 4) Aplica De/Para por descrição e marca — lote próprio.
SELECT @CMD = '
    UPDATE a
    SET a.ModeloVeiculo_Codigo = b.ModeloVeiculo_Codigo,
        a.ModeloVeiculo_Descricao = b.ModeloVeiculo_Descricao,
        a.ModeloVeiculo_ModeloMarca = b.ModeloVeiculo_ModeloMarca
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ModeloVeiculo_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.ModeloVeiculo b ON (
        RTRIM(LTRIM(b.ModeloVeiculo_Descricao)) = RTRIM(LTRIM(a.mod_ds)) COLLATE Latin1_General_CI_AI
        AND a.ModeloVeiculo_MarcaCod = b.ModeloVeiculo_MarcaCod
    )
    WHERE ISNUMERIC(a.ModeloVeiculo_Codigo) = 0
      AND ISNUMERIC(a.ModeloVeiculo_MarcaCod) = 1
'
EXEC sp_executesql @CMD
GO
-- <<< FIM: up_01_Veiculo_DePara_ModeloVeiculo.sql
GO


-- >>> INICIO: up_02_Veiculo_DePara_CorExterna.sql
-- =============================================================================
-- Layout: Veiculo De/Para — CorExterna
-- Procedure: up_02_Veiculo_DePara_CorExterna (@BancoDadosGX, @BancoWF)
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_02_Veiculo_DePara_CorExterna' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_02_Veiculo_DePara_CorExterna;
GO

CREATE PROCEDURE dbo.up_02_Veiculo_DePara_CorExterna
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX)
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

SELECT @CMD = '
    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.CorExterna_DePara
        (cor_cdext, cor_ds, Cor_Codigo, Cor_Descricao)
    SELECT DISTINCT
        cor_cdext = ISNULL(a.COR_EXTERNA_CODIGO, ''''),
        cor_ds = ISNULL(a.COR_EXTERNA_DESCRICAO, ''''),
        Cor_Codigo = ''S/DePara'',
        Cor_Descricao = ''S/DePara''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
    WHERE a.COR_EXTERNA_CODIGO IS NOT NULL
        AND a.COR_EXTERNA_CODIGO <> ''''
        AND a.Flag = 1
        AND NOT EXISTS (
            SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.CorExterna_DePara b
            WHERE ISNULL(a.COR_EXTERNA_CODIGO, '''') = ISNULL(b.cor_cdext, '''')
        )

    UPDATE a
    SET a.Cor_Codigo = b.Cor_Codigo,
        a.Cor_Descricao = b.Cor_Descricao
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.CorExterna_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Cor b ON b.Cor_Descricao = a.cor_ds COLLATE Latin1_General_CI_AI AND b.Cor_Tipo <> ''I''
    WHERE ISNUMERIC(a.Cor_Codigo) = 0
'
EXEC sp_executesql @CMD
GO
-- <<< FIM: up_02_Veiculo_DePara_CorExterna.sql
GO


-- >>> INICIO: up_03_Veiculo_DePara_CorInterna.sql
-- =============================================================================
-- Layout: Veiculo De/Para — CorInterna
-- Procedure: up_03_Veiculo_DePara_CorInterna (@BancoDadosGX, @BancoWF)
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_03_Veiculo_DePara_CorInterna' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_03_Veiculo_DePara_CorInterna;
GO

CREATE PROCEDURE dbo.up_03_Veiculo_DePara_CorInterna
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX)
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

SELECT @CMD = '
    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.CorInterna_DePara
        (cor_cd, cor_ds, Cor_Codigo, Cor_Descricao)
    SELECT DISTINCT
        cor_cd = ISNULL(a.COR_INTERNA_CODIGO, ''''),
        cor_ds = ISNULL(a.COR_INTERNA_DESCRICAO, ''''),
        Cor_Codigo = ''S/DePara'',
        Cor_Descricao = ''S/DePara''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
    WHERE a.Flag = 1
        AND NOT EXISTS (
            SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.CorInterna_DePara b
            WHERE ISNULL(a.COR_INTERNA_CODIGO, '''') = ISNULL(b.cor_cd, '''')
        )

    UPDATE a
    SET a.Cor_Codigo = b.Cor_Codigo,
        a.Cor_Descricao = b.Cor_Descricao
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.CorInterna_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Cor b ON b.Cor_Descricao = a.cor_ds COLLATE Latin1_General_CI_AI AND b.Cor_Tipo = ''I''
    WHERE ISNUMERIC(a.Cor_Codigo) = 0
'
EXEC sp_executesql @CMD
GO
-- <<< FIM: up_03_Veiculo_DePara_CorInterna.sql
GO


-- >>> INICIO: up_04_Veiculo_DePara_VeiculoAno.sql
-- =============================================================================
-- Layout: Veiculo De/Para — VeiculoAno
-- Procedure: up_04_Veiculo_DePara_VeiculoAno (@BancoDadosGX, @BancoWF)
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_04_Veiculo_DePara_VeiculoAno' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_04_Veiculo_DePara_VeiculoAno;
GO

CREATE PROCEDURE dbo.up_04_Veiculo_DePara_VeiculoAno
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX)
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

SELECT @CMD = '
    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.VeiculoAno_DePara
        (ve_fabmod, VeiculoAno_Codigo, VeiculoAno_Exibicao)
    SELECT DISTINCT
        ve_fabmod = ISNULL(a.Ve_FabMod, ''''),
        VeiculoAno_Codigo = ''S/DePara'',
        VeiculoAno_Exibicao = ''S/DePara''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
    WHERE a.Flag = 1
        AND NOT EXISTS (
            SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.VeiculoAno_DePara b
            WHERE ISNULL(a.Ve_FabMod, '''') = ISNULL(b.ve_fabmod, '''')
        )

    UPDATE a
    SET a.VeiculoAno_Codigo = b.VeiculoAno_Codigo,
        a.VeiculoAno_Exibicao = b.VeiculoAno_Exibicao
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.VeiculoAno_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.VeiculoAno b ON b.VeiculoAno_Exibicao = a.ve_fabmod COLLATE Latin1_General_CI_AI
    WHERE ISNUMERIC(a.VeiculoAno_Codigo) = 0
'
EXEC sp_executesql @CMD
GO
-- <<< FIM: up_04_Veiculo_DePara_VeiculoAno.sql
GO


-- >>> INICIO: up_05_Veiculo_DePara_Estado.sql
-- =============================================================================
-- Layout: Veiculo De/Para — Estado
-- Procedure: up_05_Veiculo_DePara_Estado (@BancoDadosGX, @BancoWF)
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_05_Veiculo_DePara_Estado' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_05_Veiculo_DePara_Estado;
GO

CREATE PROCEDURE dbo.up_05_Veiculo_DePara_Estado
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX)
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

SELECT @CMD = '
    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Estado_DePara
        (uf_cd, uf_nm, Estado_Codigo, Estado_Nome, Tabela)
    SELECT DISTINCT
        uf_cd = ISNULL(RTRIM(LTRIM(a.ESTADO_PLACA)), ''''),
        uf_nm = '''',
        Estado_Codigo = ''S/DePara'',
        Estado_Nome = ''S/DePara'',
        Tabela = ''Veiculo''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
    WHERE a.Flag = 1
        AND NOT EXISTS (
            SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Estado_DePara b
            WHERE b.UF_CD = ISNULL(RTRIM(LTRIM(a.ESTADO_PLACA)), '''') COLLATE Latin1_General_CI_AI
        )

    UPDATE a
    SET a.Estado_Codigo = b.Estado_Codigo,
        a.Estado_Nome = b.Estado_Nome
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Estado_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Estado b ON b.Estado_Codigo = a.uf_cd
    WHERE ISNUMERIC(a.Estado_Codigo) = 0

    UPDATE a
    SET a.Pais_Codigo = b.Pais_Codigo
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Estado_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Estado b ON a.Estado_Codigo = b.Estado_Codigo COLLATE database_default
    WHERE a.Pais_Codigo IS NULL
'
EXEC sp_executesql @CMD
GO
-- <<< FIM: up_05_Veiculo_DePara_Estado.sql
GO


-- >>> INICIO: up_06_Veiculo_DePara_Municipio.sql
-- =============================================================================
-- Layout: Veiculo De/Para — Municipio
-- Procedure: up_06_Veiculo_DePara_Municipio (@BancoDadosGX, @BancoWF)
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_06_Veiculo_DePara_Municipio' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_06_Veiculo_DePara_Municipio;
GO

CREATE PROCEDURE dbo.up_06_Veiculo_DePara_Municipio
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX)
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

SELECT @CMD = '
    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Municipio_DePara
        (cg_cidade, Municipio_IBGE, uf_cd, Municipio_Codigo, Municipio_Nome, Estado_Codigo, Tabela)
    SELECT DISTINCT
        cg_cidade = ISNULL(RTRIM(LTRIM(UPPER(a.MUNICIPIO_PLACA))) COLLATE SQL_Latin1_General_CP1253_CI_AI, ''''),
        Municipio_IBGE = '''',
        uf_cd = ISNULL(RTRIM(LTRIM(a.ESTADO_PLACA)), ''''),
        Municipio_Codigo = ''S/DePara'',
        Municipio_Nome = ''S/DePara'',
        Estado_Codigo = ''S/DePara'',
        Tabela = ''Veiculo''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
    WHERE a.Flag = 1
        AND NOT EXISTS (
            SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Municipio_DePara b
            WHERE RTRIM(LTRIM(ISNULL(a.MUNICIPIO_PLACA, ''''))) = RTRIM(LTRIM(ISNULL(b.cg_cidade, ''''))) COLLATE SQL_Latin1_General_CP1253_CI_AI
              AND RTRIM(LTRIM(ISNULL(a.ESTADO_PLACA, ''''))) = ISNULL(b.uf_cd, '''') COLLATE SQL_Latin1_General_CP1253_CI_AI
        )

    UPDATE a
    SET a.Municipio_Codigo = b.Municipio_Codigo,
        a.Municipio_Nome = b.Municipio_Nome,
        a.Estado_Codigo = b.Estado_Codigo
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Municipio_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Municipio b ON (
        RTRIM(LTRIM(a.cg_cidade)) = RTRIM(LTRIM(b.Municipio_Nome)) COLLATE SQL_Latin1_General_CP1253_CI_AI
        AND RTRIM(LTRIM(a.uf_cd)) = b.Estado_Codigo COLLATE SQL_Latin1_General_CP1253_CI_AI
    )
    WHERE ISNUMERIC(a.Municipio_Codigo) = 0
'
EXEC sp_executesql @CMD
GO
-- <<< FIM: up_06_Veiculo_DePara_Municipio.sql
GO


-- >>> INICIO: up_07_Veiculo_DePara_Marca.sql
-- =============================================================================
-- Layout: Veiculo De/Para — Marca
-- Procedure: up_07_Veiculo_DePara_Marca (@BancoDadosGX, @BancoWF)
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_07_Veiculo_DePara_Marca' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_07_Veiculo_DePara_Marca;
GO

CREATE PROCEDURE dbo.up_07_Veiculo_DePara_Marca
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX)
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

SELECT @CMD = '
    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Marca_DePara
        (marc_cd, marc_ds, Marca_Codigo, Marca_Descricao, Marca_Sigla)
    SELECT DISTINCT
        marc_cd = ISNULL(a.VEICULO_MARCA_CODIGO, ''''),
        marc_ds = ISNULL(a.VEICULO_MARCA_DESCRICAO, ''''),
        Marca_Codigo = ''S/DePara'',
        Marca_Descricao = ''S/DePara'',
        Marca_Sigla = ''S/DePara''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
    WHERE a.Flag = 1
        AND NOT EXISTS (
            SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Marca_DePara b
            WHERE b.marc_cd = ISNULL(a.VEICULO_MARCA_CODIGO, '''') COLLATE Latin1_General_CI_AI
        )

    UPDATE a
    SET a.Marca_Codigo = b.Marca_Codigo,
        a.Marca_Descricao = b.Marca_Descricao,
        a.Marca_Sigla = b.Marca_Sigla
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Marca_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Marca b ON b.Marca_Descricao = a.marc_ds COLLATE Latin1_General_CI_AI

    UPDATE a
    SET a.ModeloVeiculo_MarcaCod = b.Marca_Codigo
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ModeloVeiculo_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Marca_DePara b ON b.marc_cd = a.MARCA_CODIGO COLLATE database_default
    WHERE ISNUMERIC(a.ModeloVeiculo_MarcaCod) = 0
'
EXEC sp_executesql @CMD
GO
-- <<< FIM: up_07_Veiculo_DePara_Marca.sql
GO


-- >>> INICIO: up_01_Financeiro_DePara_AgenteCobrador.sql
-- =============================================================================
-- Layout: Financeiro De/Para
-- Procedure: up_01_Financeiro_DePara_AgenteCobrador (@BancoDadosGX, @BancoWF)
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Financeiro_DePara_AgenteCobrador' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_01_Financeiro_DePara_AgenteCobrador;
GO

CREATE PROCEDURE dbo.up_01_Financeiro_DePara_AgenteCobrador
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX)
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

SELECT @CMD = '
    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.AgenteCobrador_DePara
        (agc_cd, agc_nm, AgenteCobrador_Codigo, AgenteCobrador_Descricao, Origem)
    SELECT DISTINCT
        agc_cd = ISNULL(a.AGENTECOBRADOR_CODIGO, ''''),
        agc_nm = ISNULL(a.AGENTECOBRADOR_DESCRICAO, ''''),
        AgenteCobrador_Codigo = ''S/DePara'',
        AgenteCobrador_Descricao = ''S/DePara'',
        Origem = (CASE
                    WHEN (a.TIPO_MOVFINANCEIRO = ''P'') THEN ''Obrigações''
                    WHEN (a.TIPO_MOVFINANCEIRO = ''R'') THEN ''Títulos''
                    ELSE ''''
                END)
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Titulo_MG a
    WHERE
        a.Flag = 1 AND
        NOT EXISTS (
            SELECT 1
            FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.AgenteCobrador_DePara b
            WHERE ISNULL(a.AGENTECOBRADOR_CODIGO, '''') = ISNULL(b.agc_cd, '''') COLLATE DATABASE_DEFAULT
        )

    UPDATE a
    SET
        a.AgenteCobrador_Codigo = b.AgenteCobrador_Codigo,
        a.AgenteCobrador_Descricao = b.AgenteCobrador_Descricao
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.AgenteCobrador_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.AgenteCobrador b
        ON b.AgenteCobrador_Descricao = a.agc_nm COLLATE Latin1_General_CI_AI
    WHERE a.AgenteCobrador_Codigo = ''S/DePara''
'
EXEC sp_executesql @CMD
GO
-- <<< FIM: up_01_Financeiro_DePara_AgenteCobrador.sql
GO


-- >>> INICIO: up_02_Financeiro_DePara_ContaGerencial.sql
-- =============================================================================
-- Layout: Financeiro De/Para
-- Procedure: up_02_Financeiro_DePara_ContaGerencial (@BancoDadosGX, @BancoWF)
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_02_Financeiro_DePara_ContaGerencial' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_02_Financeiro_DePara_ContaGerencial;
GO

CREATE PROCEDURE dbo.up_02_Financeiro_DePara_ContaGerencial
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)
DECLARE @BancoGX SYSNAME = LTRIM(RTRIM(@BancoDadosGX))
DECLARE @BancoWFs SYSNAME = LTRIM(RTRIM(@BancoWF))

IF NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoGX)
BEGIN
    PRINT 'O < ' + @BancoGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

-- 1) Garante colunas extras em ContaGerencial_DePara (batch separado — SQL Server
--    não permite ADD + referência no mesmo batch).
SELECT @CMD = N'
    IF OBJECT_ID(N''' + QUOTENAME(@BancoGX) + N'.dbo.ContaGerencial_DePara'', N''U'') IS NULL
    BEGIN
        RAISERROR(''Tabela ContaGerencial_DePara não existe em %s'', 16, 1, ''' + @BancoGX + N''')
        RETURN
    END

    IF NOT EXISTS (
        SELECT 1 FROM ' + QUOTENAME(@BancoGX) + N'.sys.columns col
        INNER JOIN ' + QUOTENAME(@BancoGX) + N'.sys.objects obj ON col.object_id = obj.object_id
        WHERE col.name = ''ContaGerencial_Tipo'' AND obj.name = ''ContaGerencial_DePara''
    )
        ALTER TABLE ' + QUOTENAME(@BancoGX) + N'.dbo.ContaGerencial_DePara ADD ContaGerencial_Tipo char(1) NULL

    IF NOT EXISTS (
        SELECT 1 FROM ' + QUOTENAME(@BancoGX) + N'.sys.columns col
        INNER JOIN ' + QUOTENAME(@BancoGX) + N'.sys.objects obj ON col.object_id = obj.object_id
        WHERE col.name = ''ContaGerencial_Nivel'' AND obj.name = ''ContaGerencial_DePara''
    )
        ALTER TABLE ' + QUOTENAME(@BancoGX) + N'.dbo.ContaGerencial_DePara ADD ContaGerencial_Nivel char(1) NULL

    -- Colunas de origem em Titulo_MG (layout pode omitir descrição)
    IF NOT EXISTS (
        SELECT 1 FROM ' + QUOTENAME(@BancoGX) + N'.sys.columns col
        INNER JOIN ' + QUOTENAME(@BancoGX) + N'.sys.objects obj ON col.object_id = obj.object_id
        WHERE col.name = ''CONTAGERENCIAL_CODIGO'' AND obj.name = ''Titulo_MG''
    )
        ALTER TABLE ' + QUOTENAME(@BancoGX) + N'.dbo.Titulo_MG ADD CONTAGERENCIAL_CODIGO VARCHAR(MAX) NULL

    IF NOT EXISTS (
        SELECT 1 FROM ' + QUOTENAME(@BancoGX) + N'.sys.columns col
        INNER JOIN ' + QUOTENAME(@BancoGX) + N'.sys.objects obj ON col.object_id = obj.object_id
        WHERE col.name = ''CONTAGERENCIAL_DESCRICAO'' AND obj.name = ''Titulo_MG''
    )
        ALTER TABLE ' + QUOTENAME(@BancoGX) + N'.dbo.Titulo_MG ADD CONTAGERENCIAL_DESCRICAO VARCHAR(MAX) NULL
'
EXEC sp_executesql @CMD

-- 2) Carga + match WF (após as colunas existirem)
SELECT @CMD = N'
    INSERT INTO ' + QUOTENAME(@BancoGX) + N'.dbo.ContaGerencial_DePara
        (pcg_cd, pcg_ds, ContaGerencial_Codigo, ContaGerencial_Identificador, ContaGerencial_Descricao, Origem)
    SELECT DISTINCT
        pcg_cd = ISNULL(a.CONTAGERENCIAL_CODIGO, ''''),
        pcg_ds = ISNULL(a.CONTAGERENCIAL_DESCRICAO, ''''),
        ContaGerencial_Codigo = ''S/DePara'',
        ContaGerencial_Identificador = ''S/DePara'',
        ContaGerencial_Descricao = ''S/DePara'',
        Origem = (CASE
                    WHEN (a.TIPO_MOVFINANCEIRO = ''P'') THEN ''Obrigações''
                    WHEN (a.TIPO_MOVFINANCEIRO = ''R'') THEN ''Títulos''
                    ELSE ''''
                END)
    FROM ' + QUOTENAME(@BancoGX) + N'.dbo.Titulo_MG a
    WHERE
        a.Flag = 1 AND
        NOT EXISTS (
            SELECT 1
            FROM ' + QUOTENAME(@BancoGX) + N'.dbo.ContaGerencial_DePara b
            WHERE ISNULL(a.CONTAGERENCIAL_CODIGO, '''') = ISNULL(b.pcg_cd, '''') COLLATE DATABASE_DEFAULT
        )

    UPDATE a
    SET
        a.ContaGerencial_Codigo = b.ContaGerencial_Codigo,
        a.ContaGerencial_Descricao = b.ContaGerencial_Descricao,
        a.ContaGerencial_Tipo = b.ContaGerencial_Tipo,
        a.ContaGerencial_Nivel = b.ContaGerencial_Nivel
    FROM ' + QUOTENAME(@BancoGX) + N'.dbo.ContaGerencial_DePara a
    INNER JOIN ' + QUOTENAME(@BancoWFs) + N'.dbo.ContaGerencial b
        ON b.ContaGerencial_Descricao = a.pcg_ds COLLATE Latin1_General_CI_AI
    WHERE a.ContaGerencial_Codigo = ''S/DePara''
'
EXEC sp_executesql @CMD
GO
-- <<< FIM: up_02_Financeiro_DePara_ContaGerencial.sql
GO


-- >>> INICIO: up_03_Financeiro_DePara_TipoTitulo.sql
-- =============================================================================
-- Layout: Financeiro De/Para
-- Procedure: up_03_Financeiro_DePara_TipoTitulo (@BancoDadosGX, @BancoWF)
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_03_Financeiro_DePara_TipoTitulo' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_03_Financeiro_DePara_TipoTitulo;
GO

CREATE PROCEDURE dbo.up_03_Financeiro_DePara_TipoTitulo
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX)
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

SELECT @CMD = '
    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoTitulo_DePara
        (tpt_cd, tpt_ds, tpo_cd, tpo_ds, TipoTitulo_Codigo, TipoTitulo_Descricao,
         TipoTitulo_PermissaoUso, TipoTituloEmp_PessoaCod, Origem)
    SELECT DISTINCT
        tpt_cd = '''',
        tpt_ds = '''',
        tpo_cd = ISNULL(a.TIPOTITULO_CODIGO, ''''),
        tpo_ds = ISNULL(a.TIPOTITULO_DESCRICAO, ''''),
        TipoTitulo_Codigo = ''S/DePara'',
        TipoTitulo_Descricao = ''S/DePara'',
        TipoTitulo_PermissaoUso = ''P'',
        TipoTituloEmp_PessoaCod = '''',
        Origem = ''Obrigações''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Titulo_MG a
    WHERE
        a.Flag = 1 AND
        a.TIPO_MOVFINANCEIRO = ''P'' AND
        NOT EXISTS(
            SELECT 1
            FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoTitulo_DePara b
            WHERE b.tpo_cd = a.TIPOTITULO_CODIGO collate database_default AND a.TIPO_MOVFINANCEIRO = ''P''
        )

    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoTitulo_DePara
        (tpt_cd, tpt_ds, tpo_cd, tpo_ds, TipoTitulo_Codigo, TipoTitulo_Descricao,
         TipoTitulo_PermissaoUso, TipoTituloEmp_PessoaCod, Origem)
    SELECT DISTINCT
        tpt_cd = ISNULL(a.TIPOTITULO_CODIGO, ''''),
        tpt_ds = ISNULL(a.TIPOTITULO_DESCRICAO, ''''),
        tpo_cd = '''',
        tpo_ds = '''',
        TipoTitulo_Codigo = ''S/DePara'',
        TipoTitulo_Descricao = ''S/DePara'',
        TipoTitulo_PermissaoUso = ''R'',
        TipoTituloEmp_PessoaCod = '''',
        Origem = ''Títulos''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Titulo_MG a
    WHERE
        a.Flag = 1 AND
        a.TIPO_MOVFINANCEIRO = ''R'' AND
        NOT EXISTS(
            SELECT 1
            FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoTitulo_DePara b
            WHERE b.tpt_cd = a.TIPOTITULO_CODIGO collate database_default AND a.TIPO_MOVFINANCEIRO = ''R''
        )

    UPDATE a
    SET
        a.TipoTitulo_Codigo = b.TipoTitulo_Codigo,
        a.TipoTitulo_Descricao = b.TipoTitulo_Descricao,
        a.TipoTitulo_PermissaoUso = b.TipoTitulo_PermissaoUso
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoTitulo_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.TipoTitulo b ON
        b.TipoTitulo_Descricao = RTRIM(LTRIM(a.tpt_ds)) COLLATE Latin1_General_CI_AI AND
        b.TipoTitulo_PermissaoUso = a.TipoTitulo_PermissaoUso
    WHERE a.TipoTitulo_Codigo = ''S/DePara''
'
EXEC sp_executesql @CMD
GO
-- <<< FIM: up_03_Financeiro_DePara_TipoTitulo.sql
GO


-- >>> INICIO: up_04_Financeiro_DePara_Departamento.sql
-- =============================================================================
-- Layout: Financeiro De/Para
-- Procedure: up_04_Financeiro_DePara_Departamento (@BancoDadosGX, @BancoWF)
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_04_Financeiro_DePara_Departamento' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_04_Financeiro_DePara_Departamento;
GO

CREATE PROCEDURE dbo.up_04_Financeiro_DePara_Departamento
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX)
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

SELECT @CMD = '
    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Departamento_Depara
        (dep_cd, dep_nm, Departamento_Codigo, Departamento_Descricao, Departamento_Sigla)
    SELECT DISTINCT
        dep_cd = ISNULL(a.DEPARTAMENTO_CODIGO, ''''),
        dep_nm = ISNULL(a.DEPARTAMENTO_DESCRICAO, ''''),
        Departamento_Codigo = ''S/DePara'',
        Departamento_Descricao = ''S/DePara'',
        Departamento_Sigla = ''S/DePara''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Titulo_MG a
    WHERE
        NOT EXISTS (
            SELECT 1
            FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Departamento_Depara b
            WHERE b.dep_cd = ISNULL(a.DEPARTAMENTO_CODIGO, '''') collate database_default
        )

    UPDATE a
    SET
        a.Departamento_Codigo = b.Departamento_Codigo,
        a.Departamento_Descricao = b.Departamento_Descricao,
        a.Departamento_Sigla = b.Departamento_Sigla
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Departamento_Depara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Departamento b
        ON b.Departamento_Descricao = a.dep_nm COLLATE Latin1_General_CI_AI
    WHERE a.Departamento_Codigo = ''S/DePara''
'
EXEC sp_executesql @CMD
GO
-- <<< FIM: up_04_Financeiro_DePara_Departamento.sql
GO


-- >>> INICIO: up_05_Financeiro_DePara_NaturezaOperacao.sql
-- =============================================================================
-- Layout: Financeiro De/Para
-- Procedure: up_05_Financeiro_DePara_NaturezaOperacao (@BancoDadosGX, @BancoWF)
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_05_Financeiro_DePara_NaturezaOperacao' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_05_Financeiro_DePara_NaturezaOperacao;
GO

CREATE PROCEDURE dbo.up_05_Financeiro_DePara_NaturezaOperacao
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX)
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

SELECT @CMD = '
    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.NaturezaOperacao_DePara
        (me_cd, me_ds, dep_cd, Tipo, NaturezaOperacao_Codigo, NaturezaOperacao_Descricao,
         Departamento_Codigo, Procedure_Origem)
    SELECT DISTINCT
        me_cd = ISNULL(a.NATUREZAOPERACAO_CODIGO, ''''),
        me_ds = ISNULL(a.NATUREZAOPERACAO_DESCRICAO, ''''),
        dep_cd = ISNULL(a.DEPARTAMENTO_CODIGO, ''''),
        Tipo = '''',
        NaturezaOperacao_Codigo = ''S/DePara'',
        NaturezaOperacao_Descricao = ''S/DePara'',
        Departamento_Codigo = ''S/DePara'',
        Procedure_Origem = (CASE
                                WHEN (a.TIPO_MOVFINANCEIRO = ''P'') THEN ''Obrigações''
                                WHEN (a.TIPO_MOVFINANCEIRO = ''R'') THEN ''Títulos''
                                ELSE ''''
                            END)
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Titulo_MG a
    WHERE
        a.Flag = 1 AND
        NOT EXISTS (
            SELECT 1
            FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.NaturezaOperacao_DePara d
            WHERE ISNULL(d.me_cd, '''') = ISNULL(a.NATUREZAOPERACAO_CODIGO, '''') COLLATE DATABASE_DEFAULT
        )

    UPDATE a
    SET
        a.NaturezaOperacao_Codigo = b.NaturezaOperacao_Codigo,
        a.NaturezaOperacao_Descricao = b.NaturezaOperacao_Descricao
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.NaturezaOperacao_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.NaturezaOperacao b
        ON b.NaturezaOperacao_Descricao = a.me_ds COLLATE DATABASE_DEFAULT
    WHERE a.NaturezaOperacao_Codigo = ''S/DePara''

    UPDATE a
    SET a.Departamento_Codigo = b.Departamento_Codigo
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.NaturezaOperacao_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Departamento_DePara b ON b.dep_cd = a.dep_cd
    WHERE a.Departamento_Codigo = ''S/DePara''
'
EXEC sp_executesql @CMD
GO
-- <<< FIM: up_05_Financeiro_DePara_NaturezaOperacao.sql
GO


-- >>> INICIO: up_06_Financeiro_DePara_Banco.sql
-- =============================================================================
-- Layout: Financeiro De/Para
-- Procedure: up_06_Financeiro_DePara_Banco (@BancoDadosGX, @BancoWF)
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_06_Financeiro_DePara_Banco' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_06_Financeiro_DePara_Banco;
GO

CREATE PROCEDURE dbo.up_06_Financeiro_DePara_Banco
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX)
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

SELECT @CMD = '
    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Banco_DePara
        (ban_cd, ban_ds, Banco_Codigo, Banco_Descricao, Banco_Sigla)
    SELECT DISTINCT
        ban_cd = ISNULL(a.CODIGO_BANCO, ''''),
        ban_ds = '''',
        Banco_Codigo = ''S/DePara'',
        Banco_Descricao = ''S/DePara'',
        Banco_Sigla = ''S/DePara''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Titulo_MG a
    WHERE
        a.Flag = 1 AND
        NOT EXISTS (
            SELECT 1
            FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Banco_DePara b
            WHERE ISNULL(a.CODIGO_BANCO, '''') = ISNULL(b.ban_cd, '''') COLLATE DATABASE_DEFAULT
        )

    UPDATE a
    SET
        a.Banco_Codigo = b.Banco_Codigo,
        a.Banco_Descricao = b.Banco_Descricao,
        a.Banco_Sigla = b.Banco_Sigla
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Banco_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Banco b
        ON b.Banco_Sigla = a.ban_cd COLLATE Latin1_General_CI_AI
    WHERE a.Banco_Codigo = ''S/DePara''
'
EXEC sp_executesql @CMD
GO
-- <<< FIM: up_06_Financeiro_DePara_Banco.sql
GO


-- >>> INICIO: up_01_Adiantamento_DePara_TipoFichaRazao.sql
-- =============================================================================
-- Layout: Adiantamento De/Para
-- Procedure: up_01_Adiantamento_DePara_TipoFichaRazao (@BancoDadosGX, @BancoWF)
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Adiantamento_DePara_TipoFichaRazao' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_01_Adiantamento_DePara_TipoFichaRazao;
GO

CREATE PROCEDURE dbo.up_01_Adiantamento_DePara_TipoFichaRazao
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX)
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

SELECT @CMD = '
    IF OBJECT_ID(''' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoFichaRazao_DePara'', ''U'') IS NULL
    BEGIN
        RAISERROR(''Tabela TipoFichaRazao_DePara não existe.'', 16, 1)
        RETURN
    END
'
EXEC sp_executesql @CMD

-- Receber (R) → FRT; Pagar (P) → FRO (mesmo padrão de TipoTitulo)
SELECT @CMD = '
    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoFichaRazao_DePara
        (frt_cd, frt_ds, fro_cd, fro_ds, dep_cd, dep_nm,
         TipoFichaRazao_Codigo, TipoFichaRazao_Descricao, TipoFichaRazao_Natureza,
         Departamento_Codigo, Origem)
    SELECT DISTINCT
        frt_cd = ISNULL(a.TIPO_FICHARAZAO, ''''),
        frt_ds = ISNULL(a.DESCRICAO_FICHARAZAO, ''''),
        fro_cd = '''',
        fro_ds = '''',
        dep_cd = '''',
        dep_nm = '''',
        TipoFichaRazao_Codigo = ''S/DePara'',
        TipoFichaRazao_Descricao = ''S/DePara'',
        TipoFichaRazao_Natureza = a.TIPO_MOVFINANCEIRO,
        Departamento_Codigo = '''',
        Origem = ''Adiantamentos''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.FichaRazao_MG a
    WHERE
        a.Flag = 1 AND
        a.TIPO_MOVFINANCEIRO = ''R'' AND
        NOT EXISTS (
            SELECT 1
            FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoFichaRazao_DePara b
            WHERE ISNULL(b.frt_cd, '''') = ISNULL(a.TIPO_FICHARAZAO, '''') COLLATE DATABASE_DEFAULT
              AND ISNULL(b.frt_ds, '''') = ISNULL(a.DESCRICAO_FICHARAZAO, '''') COLLATE DATABASE_DEFAULT
        )

    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoFichaRazao_DePara
        (frt_cd, frt_ds, fro_cd, fro_ds, dep_cd, dep_nm,
         TipoFichaRazao_Codigo, TipoFichaRazao_Descricao, TipoFichaRazao_Natureza,
         Departamento_Codigo, Origem)
    SELECT DISTINCT
        frt_cd = '''',
        frt_ds = '''',
        fro_cd = ISNULL(a.TIPO_FICHARAZAO, ''''),
        fro_ds = ISNULL(a.DESCRICAO_FICHARAZAO, ''''),
        dep_cd = '''',
        dep_nm = '''',
        TipoFichaRazao_Codigo = ''S/DePara'',
        TipoFichaRazao_Descricao = ''S/DePara'',
        TipoFichaRazao_Natureza = a.TIPO_MOVFINANCEIRO,
        Departamento_Codigo = '''',
        Origem = ''Adiantamentos''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.FichaRazao_MG a
    WHERE
        a.Flag = 1 AND
        a.TIPO_MOVFINANCEIRO = ''P'' AND
        NOT EXISTS (
            SELECT 1
            FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoFichaRazao_DePara b
            WHERE ISNULL(b.fro_cd, '''') = ISNULL(a.TIPO_FICHARAZAO, '''') COLLATE DATABASE_DEFAULT
              AND ISNULL(b.fro_ds, '''') = ISNULL(a.DESCRICAO_FICHARAZAO, '''') COLLATE DATABASE_DEFAULT
        )

    UPDATE a
    SET
        a.TipoFichaRazao_Codigo = b.TipoFichaRazao_Codigo,
        a.TipoFichaRazao_Descricao = b.TipoFichaRazao_Descricao,
        a.TipoFichaRazao_Natureza = b.TipoFichaRazao_Natureza
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoFichaRazao_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.TipoFichaRazao b
        ON b.TipoFichaRazao_Descricao = RTRIM(LTRIM(
            CASE WHEN ISNULL(a.frt_ds, '''') <> '''' THEN a.frt_ds ELSE a.fro_ds END
        )) COLLATE Latin1_General_CI_AI
    WHERE a.TipoFichaRazao_Codigo = ''S/DePara''
'
EXEC sp_executesql @CMD
GO
-- <<< FIM: up_01_Adiantamento_DePara_TipoFichaRazao.sql
GO


-- >>> INICIO: up_01_MovimentoEstoque_DePara_NaturezaOperacao.sql
-- =============================================================================
-- Layout: MovimentoEstoque De/Para
-- Tabela : NaturezaOperacao_DePara
-- Procedure: up_01_MovimentoEstoque_DePara_NaturezaOperacao (@BancoDadosGX, @BancoWF)
-- Autor: Aroldo Santana
-- Data de alteração: 07/07/2026
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_MovimentoEstoque_DePara_NaturezaOperacao' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_01_MovimentoEstoque_DePara_NaturezaOperacao;
GO

CREATE PROCEDURE dbo.up_01_MovimentoEstoque_DePara_NaturezaOperacao
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF (NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX))
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

IF (NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoWF))
BEGIN
    PRINT 'O < ' + @BancoWF + ' > INFORMADO COMO @BancoWF NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

SELECT @CMD = '
PRINT ''==========================================================================================''
PRINT '' 01 - GERA NaturezaOperacao_DePara (origem MovimentoEstoque_MG)''
PRINT ''==========================================================================================''

    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.NaturezaOperacao_DePara
        (me_cd, me_ds, dep_cd, Tipo, NaturezaOperacao_Codigo, NaturezaOperacao_Descricao,
         Departamento_Codigo, Procedure_Origem)
    SELECT DISTINCT
        me_cd = ISNULL(a.MOVIMENTO_CODIGO, ''''),
        me_ds = ISNULL(a.MOVIMENTO_DESCRICAO, ''''),
        dep_cd = ISNULL(a.DEPARTAMENTO_CODIGO, ''''),
        Tipo = ''Historico'',
        NaturezaOperacao_Codigo = ''S/DePara'',
        NaturezaOperacao_Descricao = ''S/DePara'',
        Departamento_Codigo = ''S/DePara'',
        Procedure_Origem = ''MovimentoEstoque''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.MovimentoEstoque_MG a
    WHERE
        NOT EXISTS (
            SELECT 1
            FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.NaturezaOperacao_DePara b
            WHERE b.me_cd = a.MOVIMENTO_CODIGO COLLATE database_default
        )

PRINT ''==========================================================================================''
PRINT '' 02 - Aplica POR DESCRIÇÃO - NaturezaOperacao_DePara''
PRINT ''==========================================================================================''

    UPDATE a
    SET
        a.NaturezaOperacao_Codigo = b.NaturezaOperacao_Codigo,
        a.NaturezaOperacao_Descricao = b.NaturezaOperacao_Descricao
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.NaturezaOperacao_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.NaturezaOperacao b
        ON b.NaturezaOperacao_Descricao = a.me_ds COLLATE Latin1_General_CI_AI
    WHERE a.NaturezaOperacao_Codigo = ''S/DePara''
'
EXEC sp_executesql @CMD
GO
-- <<< FIM: up_01_MovimentoEstoque_DePara_NaturezaOperacao.sql
GO


-- >>> INICIO: up_02_MovimentoEstoque_DePara_Estoque.sql
-- =============================================================================
-- Layout: MovimentoEstoque De/Para
-- Tabela : Estoque_DePara
-- Procedure: up_02_MovimentoEstoque_DePara_Estoque (@BancoDadosGX, @BancoWF)
-- Autor: Aroldo Santana
-- Data de alteração: 07/07/2026
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_02_MovimentoEstoque_DePara_Estoque' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_02_MovimentoEstoque_DePara_Estoque;
GO

CREATE PROCEDURE dbo.up_02_MovimentoEstoque_DePara_Estoque
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF (NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX))
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

IF (NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoWF))
BEGIN
    PRINT 'O < ' + @BancoWF + ' > INFORMADO COMO @BancoWF NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

SELECT @CMD = '
PRINT ''==========================================================================================''
PRINT '' 01 - GERA Estoque_DePara (origem MovimentoEstoque_MG)''
PRINT ''==========================================================================================''

    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Estoque_DePara
        (est_cd, est_ds, Estoque_Codigo, Estoque_Descricao, Estoque_Sigla)
    SELECT DISTINCT
        est_cd = ISNULL(a.ESTOQUE_CODIGO, ''''),
        est_ds = '''',
        Estoque_Codigo = ''S/DePara'',
        Estoque_Descricao = ''S/DePara'',
        Estoque_Sigla = ''S/DePara''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.MovimentoEstoque_MG a
    WHERE
        NOT EXISTS (
            SELECT 1
            FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Estoque_DePara b
            WHERE b.est_cd = ISNULL(a.ESTOQUE_CODIGO, '''') COLLATE database_default
        )

PRINT ''==========================================================================================''
PRINT '' 02 - Aplica POR SIGLA - Estoque_DePara''
PRINT ''==========================================================================================''

    UPDATE a
    SET
        a.Estoque_Codigo = b.Estoque_Codigo,
        a.Estoque_Descricao = b.Estoque_Descricao,
        a.Estoque_Sigla = b.Estoque_Sigla,
        a.Origem = ''MovimentoEstoque''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Estoque_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Estoque b
        ON b.Estoque_Sigla = a.est_cd COLLATE Latin1_General_CI_AI
    WHERE a.Estoque_Codigo = ''S/DePara''
'
EXEC sp_executesql @CMD
GO
-- <<< FIM: up_02_MovimentoEstoque_DePara_Estoque.sql
GO


-- >>> INICIO: up_03_MovimentoEstoque_DePara_Departamento.sql
-- =============================================================================
-- Layout: MovimentoEstoque De/Para
-- Tabela : Departamento_Depara
-- Procedure: up_03_MovimentoEstoque_DePara_Departamento (@BancoDadosGX, @BancoWF)
-- Autor: Aroldo Santana
-- Data de alteração: 07/07/2026
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_03_MovimentoEstoque_DePara_Departamento' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_03_MovimentoEstoque_DePara_Departamento;
GO

CREATE PROCEDURE dbo.up_03_MovimentoEstoque_DePara_Departamento
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF (NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX))
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

IF (NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoWF))
BEGIN
    PRINT 'O < ' + @BancoWF + ' > INFORMADO COMO @BancoWF NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

SELECT @CMD = '
PRINT ''==========================================================================================''
PRINT '' 01 - GERA Departamento_Depara (origem MovimentoEstoque_MG)''
PRINT ''==========================================================================================''

    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Departamento_Depara
        (dep_cd, dep_nm, Departamento_Codigo, Departamento_Descricao, Departamento_Sigla)
    SELECT DISTINCT
        dep_cd = ISNULL(a.DEPARTAMENTO_CODIGO, ''''),
        dep_nm = ISNULL(a.DEPARTAMENTO_DESCRICAO, ''''),
        Departamento_Codigo = ''S/DePara'',
        Departamento_Descricao = ''S/DePara'',
        Departamento_Sigla = ''S/DePara''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.MovimentoEstoque_MG a
    WHERE
        NOT EXISTS (
            SELECT 1
            FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Departamento_Depara b
            WHERE b.dep_cd = ISNULL(a.DEPARTAMENTO_CODIGO, '''') COLLATE database_default
        )

PRINT ''==========================================================================================''
PRINT '' 02 - Aplica POR DESCRIÇÃO - Departamento_Depara''
PRINT ''==========================================================================================''

    UPDATE a
    SET
        a.Departamento_Codigo = b.Departamento_Codigo,
        a.Departamento_Descricao = b.Departamento_Descricao,
        a.Departamento_Sigla = b.Departamento_Sigla
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Departamento_Depara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Departamento b
        ON b.Departamento_Descricao = a.dep_nm COLLATE Latin1_General_CI_AI
    WHERE a.Departamento_Codigo = ''S/DePara''
'
EXEC sp_executesql @CMD
GO
-- <<< FIM: up_03_MovimentoEstoque_DePara_Departamento.sql
GO


-- >>> INICIO: up_01_Fseg_DePara_TipoOS.sql
-- =============================================================================
-- Layout: Fseg_Cab (+ Prd/Srv quando existirem) De/Para TipoOS
-- Procedure: up_01_Fseg_DePara_TipoOS (@BancoDadosGX, @BancoWF)
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Fseg_DePara_TipoOS' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_01_Fseg_DePara_TipoOS;
GO

CREATE PROCEDURE dbo.up_01_Fseg_DePara_TipoOS
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX)
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

-- Garante colunas usadas no insert (lote próprio)
SELECT @CMD = '
    IF NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns WHERE name = ''Origem'' AND object_id = OBJECT_ID(''' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoOS_DePara''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoOS_DePara ADD Origem varchar(50) NULL
'
EXEC sp_executesql @CMD

SELECT @CMD = '
    IF EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects WHERE type = ''U'' AND name = ''Ficha_Cab_MG'')
    BEGIN
        INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoOS_DePara
            (tpos_cd, tpos_ds, tpos_ativa, TipoOS_Codigo, TipoOS_Descricao, TipoOS_Sigla, Origem)
        SELECT DISTINCT
            tpos_cd = ISNULL(a.TIPO_OS_CODIGO, ''''),
            tpos_ds = ISNULL(a.TIPO_OS_DESCRICAO, ''''),
            tpos_ativa = '''',
            TipoOS_Codigo = ''S/DePara'',
            TipoOS_Descricao = ''S/DePara'',
            TipoOS_Sigla = ''S/DePara'',
            Origem = ''Ficha_Seguimento''
        FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG a
        WHERE NOT EXISTS (
            SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoOS_DePara b
            WHERE b.tpos_cd = a.TIPO_OS_CODIGO COLLATE DATABASE_DEFAULT
              AND b.tpos_ds = a.TIPO_OS_DESCRICAO COLLATE DATABASE_DEFAULT
        )
    END
'
EXEC sp_executesql @CMD

SELECT @CMD = '
    IF EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects WHERE type = ''U'' AND name = ''Ficha_Prd_MG'')
    BEGIN
        INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoOS_DePara
            (tpos_cd, tpos_ds, tpos_ativa, TipoOS_Codigo, TipoOS_Descricao, TipoOS_Sigla, Origem)
        SELECT DISTINCT
            tpos_cd = ISNULL(a.TIPO_OS_CODIGO, ''''),
            tpos_ds = ISNULL(a.TIPO_OS_DESCRICAO, ''''),
            tpos_ativa = '''',
            TipoOS_Codigo = ''S/DePara'',
            TipoOS_Descricao = ''S/DePara'',
            TipoOS_Sigla = ''S/DePara'',
            Origem = ''Ficha_Seguimento''
        FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG a
        WHERE NOT EXISTS (
            SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoOS_DePara b
            WHERE b.tpos_cd = a.TIPO_OS_CODIGO COLLATE DATABASE_DEFAULT
              AND b.tpos_ds = a.TIPO_OS_DESCRICAO COLLATE DATABASE_DEFAULT
        )
    END
'
EXEC sp_executesql @CMD

SELECT @CMD = '
    IF EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects WHERE type = ''U'' AND name = ''Ficha_Srv_MG'')
    BEGIN
        INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoOS_DePara
            (tpos_cd, tpos_ds, tpos_ativa, TipoOS_Codigo, TipoOS_Descricao, TipoOS_Sigla, Origem)
        SELECT DISTINCT
            tpos_cd = ISNULL(a.TIPO_OS_CODIGO, ''''),
            tpos_ds = ISNULL(a.TIPO_OS_DESCRICAO, ''''),
            tpos_ativa = '''',
            TipoOS_Codigo = ''S/DePara'',
            TipoOS_Descricao = ''S/DePara'',
            TipoOS_Sigla = ''S/DePara'',
            Origem = ''Ficha_Seguimento''
        FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG a
        WHERE NOT EXISTS (
            SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoOS_DePara b
            WHERE b.tpos_cd = a.TIPO_OS_CODIGO COLLATE DATABASE_DEFAULT
              AND b.tpos_ds = a.TIPO_OS_DESCRICAO COLLATE DATABASE_DEFAULT
        )
    END
'
EXEC sp_executesql @CMD

SELECT @CMD = '
    UPDATE a
    SET a.TipoOS_Codigo = b.TipoOS_Codigo,
        a.TipoOS_Descricao = b.TipoOS_Descricao,
        a.TipoOS_Sigla = b.TipoOS_Sigla
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoOS_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.TipoOS b
        ON b.TipoOS_Sigla = a.tpos_cd
       AND b.TipoOS_Descricao = a.tpos_ds COLLATE Latin1_General_CI_AI
    WHERE a.TipoOS_Codigo = ''S/DePara''
'
EXEC sp_executesql @CMD
GO
-- <<< FIM: up_01_Fseg_DePara_TipoOS.sql
GO


PRINT '=============================================================================';
PRINT 'Instalacao concluida.';
PRINT 'Execute agora (troque pelo nome real do banco):';
PRINT 'EXEC dbo.up_Replace_Name_DadosGx_Procedures @BancoDadosGX = ''NomeExatoDoSeuBanco'';';
PRINT '=============================================================================';
GO
