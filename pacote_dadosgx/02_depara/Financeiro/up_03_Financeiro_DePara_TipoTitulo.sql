-- =============================================================================
-- Layout: Financeiro De/Para
-- Procedure: up_03_Financeiro_DePara_TipoTitulo (@BancoDadosGX, @BancoWF)
-- =============================================================================

USE [DadosGX_SeuProjeto];  -- ALTERE para o nome real do banco DadosGX
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_03_Financeiro_DePara_TipoTitulo' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_03_Financeiro_DePara_TipoTitulo;
GO

CREATE PROCEDURE dbo.up_03_Financeiro_DePara_TipoTitulo
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
    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoTitulo_DePara
        (tpt_cd, tpt_ds, tpo_cd, tpo_ds, TipoTitulo_Codigo, TipoTitulo_Descricao,
         TipoTitulo_PermissaoUso, TipoTituloEmp_PessoaCod, Origem)
    SELECT DISTINCT
        tpt_cd = '''',
        tpt_ds = '''',
        tpo_cd = ISNULL(a.TIPOTITULO_CODIGO, ''''),
        tpo_ds = ISNULL(a.TIPOTITULO_DESCRICAO, ''''),
        TipoTitulo_Codigo = ''S/DePara'',
        TipoTitulo_Descricao = ''S/DePara'',
        TipoTitulo_PermissaoUso = ''P'',
        TipoTituloEmp_PessoaCod = '''',
        Origem = ''Obrigações''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Titulo_MG a
    WHERE
        a.Flag = 1 AND
        a.TIPO_MOVFINANCEIRO = ''P'' AND
        NOT EXISTS(
            SELECT 1
            FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoTitulo_DePara b
            WHERE b.tpo_cd = a.TIPOTITULO_CODIGO collate database_default AND a.TIPO_MOVFINANCEIRO = ''P''
        )

    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoTitulo_DePara
        (tpt_cd, tpt_ds, tpo_cd, tpo_ds, TipoTitulo_Codigo, TipoTitulo_Descricao,
         TipoTitulo_PermissaoUso, TipoTituloEmp_PessoaCod, Origem)
    SELECT DISTINCT
        tpt_cd = ISNULL(a.TIPOTITULO_CODIGO, ''''),
        tpt_ds = ISNULL(a.TIPOTITULO_DESCRICAO, ''''),
        tpo_cd = '''',
        tpo_ds = '''',
        TipoTitulo_Codigo = ''S/DePara'',
        TipoTitulo_Descricao = ''S/DePara'',
        TipoTitulo_PermissaoUso = ''R'',
        TipoTituloEmp_PessoaCod = '''',
        Origem = ''Títulos''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Titulo_MG a
    WHERE
        a.Flag = 1 AND
        a.TIPO_MOVFINANCEIRO = ''R'' AND
        NOT EXISTS(
            SELECT 1
            FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoTitulo_DePara b
            WHERE b.tpt_cd = a.TIPOTITULO_CODIGO collate database_default AND a.TIPO_MOVFINANCEIRO = ''R''
        )

    UPDATE a
    SET
        a.TipoTitulo_Codigo = b.TipoTitulo_Codigo,
        a.TipoTitulo_Descricao = b.TipoTitulo_Descricao,
        a.TipoTitulo_PermissaoUso = b.TipoTitulo_PermissaoUso
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoTitulo_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.TipoTitulo b ON
        b.TipoTitulo_Descricao = RTRIM(LTRIM(a.tpt_ds)) COLLATE Latin1_General_CI_AI AND
        b.TipoTitulo_PermissaoUso = a.TipoTitulo_PermissaoUso
    WHERE a.TipoTitulo_Codigo = ''S/DePara''
'
EXEC sp_executesql @CMD
GO
