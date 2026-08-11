-- =============================================================================
-- Layout: Veiculo De/Para — VeiculoAno
-- Procedure: up_04_Veiculo_DePara_VeiculoAno (@BancoDadosGX, @BancoWF)
-- =============================================================================

USE [DadosGX_SeuProjeto];  -- ALTERE para o nome real do banco DadosGX
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_04_Veiculo_DePara_VeiculoAno' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_04_Veiculo_DePara_VeiculoAno;
GO

CREATE PROCEDURE dbo.up_04_Veiculo_DePara_VeiculoAno
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
    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.VeiculoAno_DePara
        (ve_fabmod, VeiculoAno_Codigo, VeiculoAno_Exibicao)
    SELECT DISTINCT
        ve_fabmod = ISNULL(a.Ve_FabMod, ''''),
        VeiculoAno_Codigo = ''S/DePara'',
        VeiculoAno_Exibicao = ''S/DePara''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
    WHERE a.Flag = 1
        AND NOT EXISTS (
            SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.VeiculoAno_DePara b
            WHERE ISNULL(a.Ve_FabMod, '''') = ISNULL(b.ve_fabmod, '''')
        )

    UPDATE a
    SET a.VeiculoAno_Codigo = b.VeiculoAno_Codigo,
        a.VeiculoAno_Exibicao = b.VeiculoAno_Exibicao
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.VeiculoAno_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.VeiculoAno b ON b.VeiculoAno_Exibicao = a.ve_fabmod COLLATE Latin1_General_CI_AI
    WHERE ISNUMERIC(a.VeiculoAno_Codigo) = 0
'
EXEC sp_executesql @CMD
GO
