-- =============================================================================
-- Layout: 8 ProdutoEstoque
-- Staging : Arquivo_ProdutoEstoque_Tratado
-- Destino : ProdutoEstoque_MG
-- Procedure: up_01_Extrai_ProdutoEstoque_gx (@BancoDadosGX)
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================

USE [DadosGX_SeuProjeto];  -- ALTERE para o nome real do banco DadosGX
GO

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
