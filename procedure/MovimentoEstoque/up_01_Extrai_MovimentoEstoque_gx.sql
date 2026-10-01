-- =============================================================================
-- Layout: MovimentoEstoque
-- Staging : Arquivo_MovimentoEstoque_Tratado
-- Destino : MovimentoEstoque_MG
-- Procedure: up_01_Extrai_MovimentoEstoque_gx (@BancoDadosGX, @BancoWF)
-- Autor: Aroldo Santana
-- Data de alteração: 07/07/2026
-- =============================================================================

USE [DadosGX_Simulacao];  -- ALTERE
GO

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
