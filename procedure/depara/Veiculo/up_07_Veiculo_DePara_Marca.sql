-- =============================================================================
-- Layout: Veiculo De/Para — Marca
-- Procedure: up_07_Veiculo_DePara_Marca (@BancoDadosGX, @BancoWF)
-- =============================================================================

USE [DadosGX_Simulacao];  -- ALTERE
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_07_Veiculo_DePara_Marca' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_07_Veiculo_DePara_Marca;
GO

CREATE PROCEDURE dbo.up_07_Veiculo_DePara_Marca
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
    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Marca_DePara
        (marc_cd, marc_ds, Marca_Codigo, Marca_Descricao, Marca_Sigla)
    SELECT DISTINCT
        marc_cd = ISNULL(a.VEICULO_MARCA_CODIGO, ''''),
        marc_ds = ISNULL(a.VEICULO_MARCA_DESCRICAO, ''''),
        Marca_Codigo = ''S/DePara'',
        Marca_Descricao = ''S/DePara'',
        Marca_Sigla = ''S/DePara''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
    WHERE a.Flag = 1
        AND NOT EXISTS (
            SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Marca_DePara b
            WHERE b.marc_cd = ISNULL(a.VEICULO_MARCA_CODIGO, '''') COLLATE Latin1_General_CI_AI
        )

    UPDATE a
    SET a.Marca_Codigo = b.Marca_Codigo,
        a.Marca_Descricao = b.Marca_Descricao,
        a.Marca_Sigla = b.Marca_Sigla
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Marca_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Marca b ON b.Marca_Descricao = a.marc_ds COLLATE Latin1_General_CI_AI

    UPDATE a
    SET a.ModeloVeiculo_MarcaCod = b.Marca_Codigo
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ModeloVeiculo_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Marca_DePara b ON b.marc_cd = a.MARCA_CODIGO COLLATE database_default
    WHERE ISNUMERIC(a.ModeloVeiculo_MarcaCod) = 0
'
EXEC sp_executesql @CMD
GO
