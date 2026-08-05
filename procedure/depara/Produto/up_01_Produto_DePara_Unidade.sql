-- =============================================================================
-- Layout: 7 Produto De/Para
-- Tabela : Unidade_DePara
-- Procedure: up_01_Produto_DePara_Unidade (@BancoDadosGX, @BancoWF)
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================

USE [DadosGX_Simulacao];  -- ALTERE
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Produto_DePara_Unidade' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_01_Produto_DePara_Unidade;
GO

CREATE PROCEDURE dbo.up_01_Produto_DePara_Unidade
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
PRINT '' 01 - GERA Unidade_DePara '' 
PRINT ''==========================================================================================''

    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Unidade_DePara
        (pdund_cd, pdund_ds, Unidade_Codigo, Unidade_Descricao)
    SELECT DISTINCT
        pdund_cd = ISNULL(a.UNIDADE_PRODUTO_CODIGO, ''''),
        pdund_ds = ISNULL(a.UNIDADE_PRODUTO_DESCRICAO, ''''),
        Unidade_Codigo = ''S/DePara'',
        Unidade_Descricao = ''S/DePara''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG a
    WHERE
        a.Flag = 1 AND
        NOT EXISTS (
            SELECT 1
            FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Unidade_DePara b
            WHERE ISNULL(a.UNIDADE_PRODUTO_CODIGO, '''') = ISNULL(b.pdund_cd, '''')
        )
'
EXEC sp_executesql @CMD

SELECT @CMD = '

PRINT ''=========================================================================================='' 
PRINT '' 02 - Aplica POR DESCRICAO - Unidade_DePara '' 
PRINT ''==========================================================================================''

    UPDATE a
    SET
        a.Unidade_Codigo = b.Unidade_Codigo,
        a.Unidade_Descricao = b.Unidade_Descricao
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Unidade_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Unidade b ON b.Unidade_Descricao = a.pdund_ds COLLATE Latin1_General_CI_AI
    WHERE
        a.Unidade_Codigo = ''S/DePara''
'
EXEC sp_executesql @CMD
GO
