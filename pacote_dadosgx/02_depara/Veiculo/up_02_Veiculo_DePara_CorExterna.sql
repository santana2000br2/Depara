-- =============================================================================
-- Layout: Veiculo De/Para — CorExterna
-- Procedure: up_02_Veiculo_DePara_CorExterna (@BancoDadosGX, @BancoWF)
-- =============================================================================

USE [DadosGX_SeuProjeto];  -- ALTERE para o nome real do banco DadosGX
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_02_Veiculo_DePara_CorExterna' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_02_Veiculo_DePara_CorExterna;
GO

CREATE PROCEDURE dbo.up_02_Veiculo_DePara_CorExterna
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
    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.CorExterna_DePara
        (cor_cdext, cor_ds, Cor_Codigo, Cor_Descricao)
    SELECT DISTINCT
        cor_cdext = ISNULL(a.COR_EXTERNA_CODIGO, ''''),
        cor_ds = ISNULL(a.COR_EXTERNA_DESCRICAO, ''''),
        Cor_Codigo = ''S/DePara'',
        Cor_Descricao = ''S/DePara''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
    WHERE a.COR_EXTERNA_CODIGO IS NOT NULL
        AND a.COR_EXTERNA_CODIGO <> ''''
        AND a.Flag = 1
        AND NOT EXISTS (
            SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.CorExterna_DePara b
            WHERE ISNULL(a.COR_EXTERNA_CODIGO, '''') = ISNULL(b.cor_cdext, '''')
        )

    UPDATE a
    SET a.Cor_Codigo = b.Cor_Codigo,
        a.Cor_Descricao = b.Cor_Descricao
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.CorExterna_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Cor b ON b.Cor_Descricao = a.cor_ds COLLATE Latin1_General_CI_AI AND b.Cor_Tipo <> ''I''
    WHERE ISNUMERIC(a.Cor_Codigo) = 0
'
EXEC sp_executesql @CMD
GO
