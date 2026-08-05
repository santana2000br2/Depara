-- =============================================================================
-- Layout: 8 ProdutoEstoque De/Para
-- Tabela : Estoque_DePara
-- Procedure: up_01_ProdutoEstoque_DePara_Estoque (@BancoDadosGX, @BancoWF)
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================

USE [DadosGX_Simulacao];  -- ALTERE
GO

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
        est_ds = RTRIM(LTRIM(a.LOCALIZACAO))
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
