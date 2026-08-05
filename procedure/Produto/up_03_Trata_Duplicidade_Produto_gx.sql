-- =============================================================================
-- Layout: 7 Produto
-- Staging : Produto_MG
-- Destino : Produto_MG
-- Procedure: up_03_Trata_Duplicidade_Produto_gx (@BancoDadosGX, @BancoWF)
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================

USE [DadosGX_Simulacao];  -- ALTERE
GO

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
