-- =============================================================================
-- Layout: MovimentoEstoque De/Para
-- Tabela : Estoque_DePara
-- Procedure: up_02_MovimentoEstoque_DePara_Estoque (@BancoDadosGX, @BancoWF)
-- Autor: Aroldo Santana
-- Data de alteração: 07/07/2026
-- =============================================================================

USE [DadosGX_SeuProjeto];  -- ALTERE para o nome real do banco DadosGX
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_02_MovimentoEstoque_DePara_Estoque' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_02_MovimentoEstoque_DePara_Estoque;
GO

CREATE PROCEDURE dbo.up_02_MovimentoEstoque_DePara_Estoque
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
PRINT '' 01 - GERA Estoque_DePara (origem MovimentoEstoque_MG)''
PRINT ''==========================================================================================''

    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Estoque_DePara
        (est_cd, est_ds, Estoque_Codigo, Estoque_Descricao, Estoque_Sigla)
    SELECT DISTINCT
        est_cd = ISNULL(a.ESTOQUE_CODIGO, ''''),
        est_ds = '''',
        Estoque_Codigo = ''S/DePara'',
        Estoque_Descricao = ''S/DePara'',
        Estoque_Sigla = ''S/DePara''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.MovimentoEstoque_MG a
    WHERE
        NOT EXISTS (
            SELECT 1
            FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Estoque_DePara b
            WHERE b.est_cd = ISNULL(a.ESTOQUE_CODIGO, '''') COLLATE database_default
        )

PRINT ''==========================================================================================''
PRINT '' 02 - Aplica POR SIGLA - Estoque_DePara''
PRINT ''==========================================================================================''

    UPDATE a
    SET
        a.Estoque_Codigo = b.Estoque_Codigo,
        a.Estoque_Descricao = b.Estoque_Descricao,
        a.Estoque_Sigla = b.Estoque_Sigla,
        a.Origem = ''MovimentoEstoque''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Estoque_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Estoque b
        ON b.Estoque_Sigla = a.est_cd COLLATE Latin1_General_CI_AI
    WHERE a.Estoque_Codigo = ''S/DePara''
'
EXEC sp_executesql @CMD
GO
