-- =============================================================================
-- Layout: Financeiro De/Para
-- Procedure: up_06_Financeiro_DePara_Banco (@BancoDadosGX, @BancoWF)
-- =============================================================================

USE [DadosGX_SeuProjeto];  -- ALTERE para o nome real do banco DadosGX
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_06_Financeiro_DePara_Banco' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_06_Financeiro_DePara_Banco;
GO

CREATE PROCEDURE dbo.up_06_Financeiro_DePara_Banco
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX)
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

SELECT @CMD = '
    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Banco_DePara
        (ban_cd, ban_ds, Banco_Codigo, Banco_Descricao, Banco_Sigla)
    SELECT DISTINCT
        ban_cd = ISNULL(a.CODIGO_BANCO, ''''),
        ban_ds = '''',
        Banco_Codigo = ''S/DePara'',
        Banco_Descricao = ''S/DePara'',
        Banco_Sigla = ''S/DePara''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Titulo_MG a
    WHERE
        a.Flag = 1 AND
        NOT EXISTS (
            SELECT 1
            FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Banco_DePara b
            WHERE ISNULL(a.CODIGO_BANCO, '''') = ISNULL(b.ban_cd, '''') COLLATE DATABASE_DEFAULT
        )

    UPDATE a
    SET
        a.Banco_Codigo = b.Banco_Codigo,
        a.Banco_Descricao = b.Banco_Descricao,
        a.Banco_Sigla = b.Banco_Sigla
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Banco_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Banco b
        ON b.Banco_Sigla = a.ban_cd COLLATE Latin1_General_CI_AI
    WHERE a.Banco_Codigo = ''S/DePara''
'
EXEC sp_executesql @CMD
GO
