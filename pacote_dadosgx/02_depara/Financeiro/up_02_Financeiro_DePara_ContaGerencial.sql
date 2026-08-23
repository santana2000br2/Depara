-- =============================================================================
-- Layout: Financeiro De/Para
-- Procedure: up_02_Financeiro_DePara_ContaGerencial (@BancoDadosGX, @BancoWF)
-- =============================================================================

USE DadosGx_SINAL_GAC_AgoSet;  -- ALTERE para o nome real do banco DadosGX
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_02_Financeiro_DePara_ContaGerencial' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_02_Financeiro_DePara_ContaGerencial;
GO

CREATE PROCEDURE dbo.up_02_Financeiro_DePara_ContaGerencial
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
    IF NOT EXISTS (
        SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col
        INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id
        WHERE col.name = ''ContaGerencial_Tipo'' AND obj.name = ''ContaGerencial_DePara''
    )
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ContaGerencial_DePara ADD ContaGerencial_Tipo char(1) NULL

    IF NOT EXISTS (
        SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col
        INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id
        WHERE col.name = ''ContaGerencial_Nivel'' AND obj.name = ''ContaGerencial_DePara''
    )
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ContaGerencial_DePara ADD ContaGerencial_Nivel char(1) NULL

    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ContaGerencial_DePara
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
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Titulo_MG a
    WHERE
        a.Flag = 1 AND
        NOT EXISTS (
            SELECT 1
            FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ContaGerencial_DePara b
            WHERE ISNULL(a.CONTAGERENCIAL_CODIGO, '''') = ISNULL(b.pcg_cd, '''') COLLATE DATABASE_DEFAULT
        )

    UPDATE a
    SET
        a.ContaGerencial_Codigo = b.ContaGerencial_Codigo,
        a.ContaGerencial_Descricao = b.ContaGerencial_Descricao,
        a.ContaGerencial_Tipo = b.ContaGerencial_Tipo,
        a.ContaGerencial_Nivel = b.ContaGerencial_Nivel
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ContaGerencial_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.ContaGerencial b
        ON b.ContaGerencial_Descricao = a.pcg_ds COLLATE Latin1_General_CI_AI
    WHERE a.ContaGerencial_Codigo = ''S/DePara''
'
EXEC sp_executesql @CMD
GO
