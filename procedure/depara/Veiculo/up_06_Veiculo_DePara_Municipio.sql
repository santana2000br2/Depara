-- =============================================================================
-- Layout: Veiculo De/Para — Municipio
-- Procedure: up_06_Veiculo_DePara_Municipio (@BancoDadosGX, @BancoWF)
-- =============================================================================

USE [DadosGX_Simulacao];  -- ALTERE
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_06_Veiculo_DePara_Municipio' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_06_Veiculo_DePara_Municipio;
GO

CREATE PROCEDURE dbo.up_06_Veiculo_DePara_Municipio
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
    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Municipio_DePara
        (cg_cidade, Municipio_IBGE, uf_cd, Municipio_Codigo, Municipio_Nome, Estado_Codigo, Tabela)
    SELECT DISTINCT
        cg_cidade = ISNULL(RTRIM(LTRIM(UPPER(a.MUNICIPIO_PLACA))) COLLATE SQL_Latin1_General_CP1253_CI_AI, ''''),
        Municipio_IBGE = '''',
        uf_cd = ISNULL(RTRIM(LTRIM(a.ESTADO_PLACA)), ''''),
        Municipio_Codigo = ''S/DePara'',
        Municipio_Nome = ''S/DePara'',
        Estado_Codigo = ''S/DePara'',
        Tabela = ''Veiculo''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
    WHERE a.Flag = 1
        AND NOT EXISTS (
            SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Municipio_DePara b
            WHERE RTRIM(LTRIM(ISNULL(a.MUNICIPIO_PLACA, ''''))) = RTRIM(LTRIM(ISNULL(b.cg_cidade, ''''))) COLLATE SQL_Latin1_General_CP1253_CI_AI
              AND RTRIM(LTRIM(ISNULL(a.ESTADO_PLACA, ''''))) = ISNULL(b.uf_cd, '''') COLLATE SQL_Latin1_General_CP1253_CI_AI
        )

    UPDATE a
    SET a.Municipio_Codigo = b.Municipio_Codigo,
        a.Municipio_Nome = b.Municipio_Nome,
        a.Estado_Codigo = b.Estado_Codigo
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Municipio_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Municipio b ON (
        RTRIM(LTRIM(a.cg_cidade)) = RTRIM(LTRIM(b.Municipio_Nome)) COLLATE SQL_Latin1_General_CP1253_CI_AI
        AND RTRIM(LTRIM(a.uf_cd)) = b.Estado_Codigo COLLATE SQL_Latin1_General_CP1253_CI_AI
    )
    WHERE ISNUMERIC(a.Municipio_Codigo) = 0
'
EXEC sp_executesql @CMD
GO
