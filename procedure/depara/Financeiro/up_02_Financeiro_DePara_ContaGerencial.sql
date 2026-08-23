-- =============================================================================
-- Layout: Financeiro De/Para
-- Procedure: up_02_Financeiro_DePara_ContaGerencial (@BancoDadosGX, @BancoWF)
-- =============================================================================

USE [DadosGX_Simulacao];  -- ALTERE
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_02_Financeiro_DePara_ContaGerencial' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_02_Financeiro_DePara_ContaGerencial;
GO

CREATE PROCEDURE dbo.up_02_Financeiro_DePara_ContaGerencial
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)
DECLARE @BancoGX SYSNAME = LTRIM(RTRIM(@BancoDadosGX))
DECLARE @BancoWFs SYSNAME = LTRIM(RTRIM(@BancoWF))

IF NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoGX)
BEGIN
    PRINT 'O < ' + @BancoGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

-- 1) Garante colunas extras em ContaGerencial_DePara (batch separado — SQL Server
--    não permite ADD + referência no mesmo batch).
SELECT @CMD = N'
    IF OBJECT_ID(N''' + QUOTENAME(@BancoGX) + N'.dbo.ContaGerencial_DePara'', N''U'') IS NULL
    BEGIN
        RAISERROR(''Tabela ContaGerencial_DePara não existe em %s'', 16, 1, ''' + @BancoGX + N''')
        RETURN
    END

    IF NOT EXISTS (
        SELECT 1 FROM ' + QUOTENAME(@BancoGX) + N'.sys.columns col
        INNER JOIN ' + QUOTENAME(@BancoGX) + N'.sys.objects obj ON col.object_id = obj.object_id
        WHERE col.name = ''ContaGerencial_Tipo'' AND obj.name = ''ContaGerencial_DePara''
    )
        ALTER TABLE ' + QUOTENAME(@BancoGX) + N'.dbo.ContaGerencial_DePara ADD ContaGerencial_Tipo char(1) NULL

    IF NOT EXISTS (
        SELECT 1 FROM ' + QUOTENAME(@BancoGX) + N'.sys.columns col
        INNER JOIN ' + QUOTENAME(@BancoGX) + N'.sys.objects obj ON col.object_id = obj.object_id
        WHERE col.name = ''ContaGerencial_Nivel'' AND obj.name = ''ContaGerencial_DePara''
    )
        ALTER TABLE ' + QUOTENAME(@BancoGX) + N'.dbo.ContaGerencial_DePara ADD ContaGerencial_Nivel char(1) NULL

    -- Colunas de origem em Titulo_MG (layout pode omitir descrição)
    IF NOT EXISTS (
        SELECT 1 FROM ' + QUOTENAME(@BancoGX) + N'.sys.columns col
        INNER JOIN ' + QUOTENAME(@BancoGX) + N'.sys.objects obj ON col.object_id = obj.object_id
        WHERE col.name = ''CONTAGERENCIAL_CODIGO'' AND obj.name = ''Titulo_MG''
    )
        ALTER TABLE ' + QUOTENAME(@BancoGX) + N'.dbo.Titulo_MG ADD CONTAGERENCIAL_CODIGO VARCHAR(MAX) NULL

    IF NOT EXISTS (
        SELECT 1 FROM ' + QUOTENAME(@BancoGX) + N'.sys.columns col
        INNER JOIN ' + QUOTENAME(@BancoGX) + N'.sys.objects obj ON col.object_id = obj.object_id
        WHERE col.name = ''CONTAGERENCIAL_DESCRICAO'' AND obj.name = ''Titulo_MG''
    )
        ALTER TABLE ' + QUOTENAME(@BancoGX) + N'.dbo.Titulo_MG ADD CONTAGERENCIAL_DESCRICAO VARCHAR(MAX) NULL
'
EXEC sp_executesql @CMD

-- 2) Carga + match WF (após as colunas existirem)
SELECT @CMD = N'
    INSERT INTO ' + QUOTENAME(@BancoGX) + N'.dbo.ContaGerencial_DePara
        (pcg_cd, pcg_ds, ContaGerencial_Codigo, ContaGerencial_Identificador, ContaGerencial_Descricao, Origem)
    SELECT DISTINCT
        pcg_cd = ISNULL(a.CONTAGERENCIAL_CODIGO, ''''),
        pcg_ds = ISNULL(a.CONTAGERENCIAL_DESCRICAO, ''''),
        ContaGerencial_Codigo = ''S/DePara'',
        ContaGerencial_Identificador = ''S/DePara'',
        ContaGerencial_Descricao = ''S/DePara'',
        Origem = (CASE
                    WHEN (a.TIPO_MOVFINANCEIRO = ''P'') THEN ''Obrigações''
                    WHEN (a.TIPO_MOVFINANCEIRO = ''R'') THEN ''Títulos''
                    ELSE ''''
                END)
    FROM ' + QUOTENAME(@BancoGX) + N'.dbo.Titulo_MG a
    WHERE
        a.Flag = 1 AND
        NOT EXISTS (
            SELECT 1
            FROM ' + QUOTENAME(@BancoGX) + N'.dbo.ContaGerencial_DePara b
            WHERE ISNULL(a.CONTAGERENCIAL_CODIGO, '''') = ISNULL(b.pcg_cd, '''') COLLATE DATABASE_DEFAULT
        )

    UPDATE a
    SET
        a.ContaGerencial_Codigo = b.ContaGerencial_Codigo,
        a.ContaGerencial_Descricao = b.ContaGerencial_Descricao,
        a.ContaGerencial_Tipo = b.ContaGerencial_Tipo,
        a.ContaGerencial_Nivel = b.ContaGerencial_Nivel
    FROM ' + QUOTENAME(@BancoGX) + N'.dbo.ContaGerencial_DePara a
    INNER JOIN ' + QUOTENAME(@BancoWFs) + N'.dbo.ContaGerencial b
        ON b.ContaGerencial_Descricao = a.pcg_ds COLLATE Latin1_General_CI_AI
    WHERE a.ContaGerencial_Codigo = ''S/DePara''
'
EXEC sp_executesql @CMD
GO
