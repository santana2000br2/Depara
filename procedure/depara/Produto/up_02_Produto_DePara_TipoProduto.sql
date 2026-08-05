-- =============================================================================
-- Layout: 7 Produto De/Para
-- Tabela : TipoProduto_DePara
-- Procedure: up_02_Produto_DePara_TipoProduto (@BancoDadosGX, @BancoWF)
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================

USE [DadosGX_Simulacao];  -- ALTERE
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_02_Produto_DePara_TipoProduto' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_02_Produto_DePara_TipoProduto;
GO

CREATE PROCEDURE dbo.up_02_Produto_DePara_TipoProduto
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
PRINT '' 01 - GERA TipoProduto_DePara '' 
PRINT ''==========================================================================================''

    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoProduto_DePara
        (tpd_cd, tpd_ds, TipoProduto_Codigo, TipoProduto_Descricao, TipoProduto_GrupoContabilCod)
    SELECT DISTINCT
        tpd_cd = ISNULL(a.TIPO_PRODUTO_CODIGO, ''''),
        tpd_ds = ISNULL(a.TIPO_PRODUTO_DESCRICAO, ''''),
        TipoProduto_Codigo = ''S/DePara'',
        TipoProduto_Descricao = ''S/DePara'',
        TipoProduto_GrupoContabilCod = ''S/DePara''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG a
    WHERE
        a.Flag = 1 AND
        NOT EXISTS (
            SELECT 1
            FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoProduto_DePara b
            WHERE ISNULL(a.TIPO_PRODUTO_CODIGO, '''') = ISNULL(b.tpd_cd, '''')
        )
'
EXEC sp_executesql @CMD

SELECT @CMD = '

PRINT ''=========================================================================================='' 
PRINT '' 02 - Aplica POR DESCRICAO - TipoProduto_DePara '' 
PRINT ''==========================================================================================''

    UPDATE a
    SET
        a.TipoProduto_Codigo = b.TipoProduto_Codigo,
        a.TipoProduto_Descricao = b.TipoProduto_Descricao,
        a.TipoProduto_GrupoContabilCod = b.TipoProduto_GrupoContabilCod
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoProduto_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.TipoProduto b ON b.TipoProduto_Descricao = a.tpd_ds COLLATE Latin1_General_CI_AI
    WHERE
        a.TipoProduto_Codigo = ''S/DePara''
'
EXEC sp_executesql @CMD
GO
