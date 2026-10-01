-- =============================================================================
-- Layout: Adiantamento De/Para
-- Procedure: up_01_Adiantamento_DePara_TipoFichaRazao (@BancoDadosGX, @BancoWF)
-- =============================================================================

USE [DadosGX_Simulacao];  -- ALTERE
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Adiantamento_DePara_TipoFichaRazao' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_01_Adiantamento_DePara_TipoFichaRazao;
GO

CREATE PROCEDURE dbo.up_01_Adiantamento_DePara_TipoFichaRazao
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
    IF OBJECT_ID(''' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoFichaRazao_DePara'', ''U'') IS NULL
    BEGIN
        RAISERROR(''Tabela TipoFichaRazao_DePara não existe.'', 16, 1)
        RETURN
    END
'
EXEC sp_executesql @CMD

-- Receber (R) → FRT; Pagar (P) → FRO (mesmo padrão de TipoTitulo)
SELECT @CMD = '
    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoFichaRazao_DePara
        (frt_cd, frt_ds, fro_cd, fro_ds, dep_cd, dep_nm,
         TipoFichaRazao_Codigo, TipoFichaRazao_Descricao, TipoFichaRazao_Natureza,
         Departamento_Codigo, Origem)
    SELECT DISTINCT
        frt_cd = ISNULL(a.TIPO_FICHARAZAO, ''''),
        frt_ds = ISNULL(a.DESCRICAO_FICHARAZAO, ''''),
        fro_cd = '''',
        fro_ds = '''',
        dep_cd = '''',
        dep_nm = '''',
        TipoFichaRazao_Codigo = ''S/DePara'',
        TipoFichaRazao_Descricao = ''S/DePara'',
        TipoFichaRazao_Natureza = a.TIPO_MOVFINANCEIRO,
        Departamento_Codigo = '''',
        Origem = ''Adiantamentos''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.FichaRazao_MG a
    WHERE
        a.Flag = 1 AND
        a.TIPO_MOVFINANCEIRO = ''R'' AND
        NOT EXISTS (
            SELECT 1
            FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoFichaRazao_DePara b
            WHERE ISNULL(b.frt_cd, '''') = ISNULL(a.TIPO_FICHARAZAO, '''') COLLATE DATABASE_DEFAULT
              AND ISNULL(b.frt_ds, '''') = ISNULL(a.DESCRICAO_FICHARAZAO, '''') COLLATE DATABASE_DEFAULT
        )

    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoFichaRazao_DePara
        (frt_cd, frt_ds, fro_cd, fro_ds, dep_cd, dep_nm,
         TipoFichaRazao_Codigo, TipoFichaRazao_Descricao, TipoFichaRazao_Natureza,
         Departamento_Codigo, Origem)
    SELECT DISTINCT
        frt_cd = '''',
        frt_ds = '''',
        fro_cd = ISNULL(a.TIPO_FICHARAZAO, ''''),
        fro_ds = ISNULL(a.DESCRICAO_FICHARAZAO, ''''),
        dep_cd = '''',
        dep_nm = '''',
        TipoFichaRazao_Codigo = ''S/DePara'',
        TipoFichaRazao_Descricao = ''S/DePara'',
        TipoFichaRazao_Natureza = a.TIPO_MOVFINANCEIRO,
        Departamento_Codigo = '''',
        Origem = ''Adiantamentos''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.FichaRazao_MG a
    WHERE
        a.Flag = 1 AND
        a.TIPO_MOVFINANCEIRO = ''P'' AND
        NOT EXISTS (
            SELECT 1
            FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoFichaRazao_DePara b
            WHERE ISNULL(b.fro_cd, '''') = ISNULL(a.TIPO_FICHARAZAO, '''') COLLATE DATABASE_DEFAULT
              AND ISNULL(b.fro_ds, '''') = ISNULL(a.DESCRICAO_FICHARAZAO, '''') COLLATE DATABASE_DEFAULT
        )

    UPDATE a
    SET
        a.TipoFichaRazao_Codigo = b.TipoFichaRazao_Codigo,
        a.TipoFichaRazao_Descricao = b.TipoFichaRazao_Descricao,
        a.TipoFichaRazao_Natureza = b.TipoFichaRazao_Natureza
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoFichaRazao_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.TipoFichaRazao b
        ON b.TipoFichaRazao_Descricao = RTRIM(LTRIM(
            CASE WHEN ISNULL(a.frt_ds, '''') <> '''' THEN a.frt_ds ELSE a.fro_ds END
        )) COLLATE Latin1_General_CI_AI
    WHERE a.TipoFichaRazao_Codigo = ''S/DePara''
'
EXEC sp_executesql @CMD
GO
