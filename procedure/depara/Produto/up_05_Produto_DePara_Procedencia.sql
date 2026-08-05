-- =============================================================================
-- Layout: 7 Produto De/Para
-- Tabela : Procedencia_DePara
-- Procedure: up_05_Produto_DePara_Procedencia (@BancoDadosGX, @BancoWF)
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================

USE [DadosGX_Simulacao];  -- ALTERE
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_05_Produto_DePara_Procedencia' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_05_Produto_DePara_Procedencia;
GO

CREATE PROCEDURE dbo.up_05_Produto_DePara_Procedencia
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
PRINT '' 01 - GERA Procedencia_DePara '' 
PRINT ''==========================================================================================''

    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Procedencia_DePara
        (pro_cd, pro_ds, Procedencia_Codigo, Procedencia_Descricao)
    SELECT DISTINCT
        pro_cd = ISNULL(a.PROCEDENCIA_CODIGO, ''''),
        pro_ds = ISNULL(a.PROCEDENCIA_DESCRICAO, ''''),
        Procedencia_Codigo = ''S/DePara'',
        Procedencia_Descricao = ''S/DePara''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG a
    WHERE
        a.Flag = 1 AND
        NOT EXISTS (
            SELECT 1
            FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Procedencia_DePara b
            WHERE ISNULL(a.PROCEDENCIA_CODIGO, '''') = ISNULL(b.pro_cd, '''')
        )
'
EXEC sp_executesql @CMD

SELECT @CMD = '

PRINT ''=========================================================================================='' 
PRINT '' 02 - Aplica POR DESCRICAO - Procedencia_DePara '' 
PRINT ''==========================================================================================''

    UPDATE a
    SET
        a.Procedencia_Codigo = b.Procedencia_Codigo,
        a.Procedencia_Descricao = b.Procedencia_Descricao
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Procedencia_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Procedencia b ON b.Procedencia_Descricao = a.pro_ds COLLATE Latin1_General_CI_AI
    WHERE
        a.Procedencia_Codigo = ''S/DePara''
'
EXEC sp_executesql @CMD
GO
