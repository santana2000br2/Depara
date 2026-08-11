-- =============================================================================
-- Layout: Veiculo De/Para — CorInterna
-- Procedure: up_03_Veiculo_DePara_CorInterna (@BancoDadosGX, @BancoWF)
-- =============================================================================

USE [DadosGX_SeuProjeto];  -- ALTERE para o nome real do banco DadosGX
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_03_Veiculo_DePara_CorInterna' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_03_Veiculo_DePara_CorInterna;
GO

CREATE PROCEDURE dbo.up_03_Veiculo_DePara_CorInterna
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
    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.CorInterna_DePara
        (cor_cd, cor_ds, Cor_Codigo, Cor_Descricao)
    SELECT DISTINCT
        cor_cd = ISNULL(a.COR_INTERNA_CODIGO, ''''),
        cor_ds = ISNULL(a.COR_INTERNA_DESCRICAO, ''''),
        Cor_Codigo = ''S/DePara'',
        Cor_Descricao = ''S/DePara''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
    WHERE a.Flag = 1
        AND NOT EXISTS (
            SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.CorInterna_DePara b
            WHERE ISNULL(a.COR_INTERNA_CODIGO, '''') = ISNULL(b.cor_cd, '''')
        )

    UPDATE a
    SET a.Cor_Codigo = b.Cor_Codigo,
        a.Cor_Descricao = b.Cor_Descricao
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.CorInterna_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Cor b ON b.Cor_Descricao = a.cor_ds COLLATE Latin1_General_CI_AI AND b.Cor_Tipo = ''I''
    WHERE ISNUMERIC(a.Cor_Codigo) = 0
'
EXEC sp_executesql @CMD
GO
