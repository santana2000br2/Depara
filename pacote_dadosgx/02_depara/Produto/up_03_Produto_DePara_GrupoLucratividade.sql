-- =============================================================================
-- Layout: 7 Produto De/Para
-- Tabela : GrupoLucratividade_DePara
-- Procedure: up_03_Produto_DePara_GrupoLucratividade (@BancoDadosGX, @BancoWF)
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================

USE [DadosGX_SeuProjeto];  -- ALTERE para o nome real do banco DadosGX
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_03_Produto_DePara_GrupoLucratividade' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_03_Produto_DePara_GrupoLucratividade;
GO

CREATE PROCEDURE dbo.up_03_Produto_DePara_GrupoLucratividade
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
PRINT '' 01 - GERA GrupoLucratividade_DePara '' 
PRINT ''==========================================================================================''

    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.GrupoLucratividade_DePara
        (letr_cd, letr_ds, letr_cdmont, GrupoLucratividade_Codigo, GrupoLucratividade_Descricao, GrupoLucratividade_MarcaCod, GrupoLucratividade_Letra)
    SELECT DISTINCT
        letr_cd = ISNULL(a.GRUPO_LUCRATIVIDADE_CODIGO, ''''),
        letr_ds = ISNULL(a.GRUPO_LUCRATIVIDADE_DESCRICAO, ''''),
        letr_cdmont = '''',
        GrupoLucratividade_Codigo = ''S/DePara'',
        GrupoLucratividade_Descricao = ''S/DePara'',
        GrupoLucratividade_MarcaCod = ISNULL(a.ProdutoMarca_MarcaCod, ''''),
        GrupoLucratividade_Letra = ''S/DePara''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG a
    WHERE
        a.Flag = 1 AND
        NOT EXISTS (
            SELECT 1
            FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.GrupoLucratividade_DePara b
            WHERE
                ISNULL(a.GRUPO_LUCRATIVIDADE_CODIGO, '''') = ISNULL(b.letr_cd, '''') AND
                ISNULL(a.GRUPO_LUCRATIVIDADE_DESCRICAO, '''') = ISNULL(b.letr_ds, '''') AND
                a.ProdutoMarca_MarcaCod = b.GrupoLucratividade_MarcaCod
        )
'
EXEC sp_executesql @CMD

SELECT @CMD = '

PRINT ''=========================================================================================='' 
PRINT '' 02 - Aplica POR DESCRICAO - GrupoLucratividade_DePara '' 
PRINT ''==========================================================================================''

    UPDATE a
    SET
        a.GrupoLucratividade_Codigo = b.GrupoLucratividade_Codigo,
        a.GrupoLucratividade_Descricao = b.GrupoLucratividade_Descricao,
        a.GrupoLucratividade_MarcaCod = b.GrupoLucratividade_MarcaCod,
        a.GrupoLucratividade_Letra = b.GrupoLucratividade_Letra
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.GrupoLucratividade_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.GrupoLucratividade b ON
        b.GrupoLucratividade_Letra = RTRIM(LTRIM(a.letr_cd)) COLLATE Latin1_General_CI_AI AND
        a.GrupoLucratividade_MarcaCod = b.GrupoLucratividade_MarcaCod
    WHERE
        a.GrupoLucratividade_Codigo = ''S/DePara''
'
EXEC sp_executesql @CMD
GO
