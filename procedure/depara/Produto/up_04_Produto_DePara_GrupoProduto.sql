-- =============================================================================
-- Layout: 7 Produto De/Para
-- Tabela : GrupoProduto_DePara
-- Procedure: up_04_Produto_DePara_GrupoProduto (@BancoDadosGX, @BancoWF)
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================

USE [DadosGX_Simulacao];  -- ALTERE
GO

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
