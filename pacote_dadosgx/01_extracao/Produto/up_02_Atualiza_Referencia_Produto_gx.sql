-- =============================================================================
-- Layout: 7 Produto
-- Staging : Produto_MG
-- Destino : Produto_MG
-- Procedure: up_02_Atualiza_Referencia_Produto_gx (@BancoDadosGX, @BancoWF)
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================

USE [DadosGX_SeuProjeto];  -- ALTERE para o nome real do banco DadosGX
GO

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
