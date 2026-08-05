-- =============================================================================
-- Layout: MovimentoEstoque De/Para
-- Tabela : NaturezaOperacao_DePara
-- Procedure: up_01_MovimentoEstoque_DePara_NaturezaOperacao (@BancoDadosGX, @BancoWF)
-- Autor: Aroldo Santana
-- Data de alteração: 07/07/2026
-- =============================================================================

USE [DadosGX_Simulacao];  -- ALTERE
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_MovimentoEstoque_DePara_NaturezaOperacao' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_01_MovimentoEstoque_DePara_NaturezaOperacao;
GO

CREATE PROCEDURE dbo.up_01_MovimentoEstoque_DePara_NaturezaOperacao
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
PRINT '' 01 - GERA NaturezaOperacao_DePara (origem MovimentoEstoque_MG)''
PRINT ''==========================================================================================''

    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.NaturezaOperacao_DePara
        (me_cd, me_ds, dep_cd, Tipo, NaturezaOperacao_Codigo, NaturezaOperacao_Descricao,
         Departamento_Codigo, Procedure_Origem)
    SELECT DISTINCT
        me_cd = ISNULL(a.MOVIMENTO_CODIGO, ''''),
        me_ds = ISNULL(a.MOVIMENTO_DESCRICAO, ''''),
        dep_cd = ISNULL(a.DEPARTAMENTO_CODIGO, ''''),
        Tipo = ''Historico'',
        NaturezaOperacao_Codigo = ''S/DePara'',
        NaturezaOperacao_Descricao = ''S/DePara'',
        Departamento_Codigo = ''S/DePara'',
        Procedure_Origem = ''MovimentoEstoque''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.MovimentoEstoque_MG a
    WHERE
        NOT EXISTS (
            SELECT 1
            FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.NaturezaOperacao_DePara b
            WHERE b.me_cd = a.MOVIMENTO_CODIGO COLLATE database_default
        )

PRINT ''==========================================================================================''
PRINT '' 02 - Aplica POR DESCRIÇÃO - NaturezaOperacao_DePara''
PRINT ''==========================================================================================''

    UPDATE a
    SET
        a.NaturezaOperacao_Codigo = b.NaturezaOperacao_Codigo,
        a.NaturezaOperacao_Descricao = b.NaturezaOperacao_Descricao
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.NaturezaOperacao_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.NaturezaOperacao b
        ON b.NaturezaOperacao_Descricao = a.me_ds COLLATE Latin1_General_CI_AI
    WHERE a.NaturezaOperacao_Codigo = ''S/DePara''
'
EXEC sp_executesql @CMD
GO
