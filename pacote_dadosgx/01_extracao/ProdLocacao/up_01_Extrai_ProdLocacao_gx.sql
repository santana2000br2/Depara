-- =============================================================================
-- Layout: ProdLocacao
-- Staging : Arquivo_ProdLocacao_Tratado
-- Destino : ProdLocacao_MG
-- Procedure: up_01_Extrai_ProdLocacao_gx (@BancoDadosGX, @BancoWF)
-- Sem De/Para
-- =============================================================================

USE [DadosGX_SeuProjeto];  -- ALTERE para o nome real do banco DadosGX
GO

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
