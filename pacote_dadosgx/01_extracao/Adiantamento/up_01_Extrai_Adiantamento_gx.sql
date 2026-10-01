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

USE [DadosGX_SeuProjeto];  -- ALTERE para o nome real do banco DadosGX
GO

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
