-- =============================================================================
-- Layout: MovimentoEstoque De/Para
-- Tabela : Departamento_Depara
-- Procedure: up_03_MovimentoEstoque_DePara_Departamento (@BancoDadosGX, @BancoWF)
-- Autor: Aroldo Santana
-- Data de alteração: 07/07/2026
-- =============================================================================

USE [DadosGX_SeuProjeto];  -- ALTERE para o nome real do banco DadosGX
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_03_MovimentoEstoque_DePara_Departamento' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_03_MovimentoEstoque_DePara_Departamento;
GO

CREATE PROCEDURE dbo.up_03_MovimentoEstoque_DePara_Departamento
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
PRINT '' 01 - GERA Departamento_Depara (origem MovimentoEstoque_MG)''
PRINT ''==========================================================================================''

    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Departamento_Depara
        (dep_cd, dep_nm, Departamento_Codigo, Departamento_Descricao, Departamento_Sigla)
    SELECT DISTINCT
        dep_cd = ISNULL(a.DEPARTAMENTO_CODIGO, ''''),
        dep_nm = ISNULL(a.DEPARTAMENTO_DESCRICAO, ''''),
        Departamento_Codigo = ''S/DePara'',
        Departamento_Descricao = ''S/DePara'',
        Departamento_Sigla = ''S/DePara''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.MovimentoEstoque_MG a
    WHERE
        NOT EXISTS (
            SELECT 1
            FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Departamento_Depara b
            WHERE b.dep_cd = ISNULL(a.DEPARTAMENTO_CODIGO, '''') COLLATE database_default
        )

PRINT ''==========================================================================================''
PRINT '' 02 - Aplica POR DESCRIÇÃO - Departamento_Depara''
PRINT ''==========================================================================================''

    UPDATE a
    SET
        a.Departamento_Codigo = b.Departamento_Codigo,
        a.Departamento_Descricao = b.Departamento_Descricao,
        a.Departamento_Sigla = b.Departamento_Sigla
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Departamento_Depara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Departamento b
        ON b.Departamento_Descricao = a.dep_nm COLLATE Latin1_General_CI_AI
    WHERE a.Departamento_Codigo = ''S/DePara''
'
EXEC sp_executesql @CMD
GO
