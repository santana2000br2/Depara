-- =============================================================================
-- Layout: Veiculo De/Para — Estado
-- Procedure: up_05_Veiculo_DePara_Estado (@BancoDadosGX, @BancoWF)
-- =============================================================================

USE [DadosGX_Simulacao];  -- ALTERE
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_05_Veiculo_DePara_Estado' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_05_Veiculo_DePara_Estado;
GO

CREATE PROCEDURE dbo.up_05_Veiculo_DePara_Estado
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
    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Estado_DePara
        (uf_cd, uf_nm, Estado_Codigo, Estado_Nome, Tabela)
    SELECT DISTINCT
        uf_cd = ISNULL(RTRIM(LTRIM(a.ESTADO_PLACA)), ''''),
        uf_nm = '''',
        Estado_Codigo = ''S/DePara'',
        Estado_Nome = ''S/DePara'',
        Tabela = ''Veiculo''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
    WHERE a.Flag = 1
        AND NOT EXISTS (
            SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Estado_DePara b
            WHERE b.UF_CD = ISNULL(RTRIM(LTRIM(a.ESTADO_PLACA)), '''') COLLATE Latin1_General_CI_AI
        )

    UPDATE a
    SET a.Estado_Codigo = b.Estado_Codigo,
        a.Estado_Nome = b.Estado_Nome
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Estado_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Estado b ON b.Estado_Codigo = a.uf_cd
    WHERE ISNUMERIC(a.Estado_Codigo) = 0

    UPDATE a
    SET a.Pais_Codigo = b.Pais_Codigo
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Estado_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Estado b ON a.Estado_Codigo = b.Estado_Codigo COLLATE database_default
    WHERE a.Pais_Codigo IS NULL
'
EXEC sp_executesql @CMD
GO
