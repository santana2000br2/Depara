-- =============================================================================
-- Layout: Financeiro
-- Staging : Arquivo_Financeiro_Tratado
-- Destino : Titulo_MG
-- Procedure: up_01_Extrai_Financeiro_gx (@BancoDadosGX, @BancoWF)
-- Depende : Pessoa_MG (CPF_CNPJ), Empresa_DePara (CNPJ_EMPRESA)
-- =============================================================================

USE [DadosGX_Simulacao];  -- ALTERE
GO

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
