-- =============================================================================
-- Layout: 7 Produto
-- Staging : Arquivo_Produto_Tratado
-- Destino : Produto_MG
-- Procedure: up_01_Extrai_Produto_gx (@BancoDadosGX, @BancoWF)
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================

USE [DadosGX_Simulacao];  -- ALTERE
GO

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
