-- =============================================================================
-- Layout: Financeiro De/Para
-- Procedure: up_01_Financeiro_DePara_AgenteCobrador (@BancoDadosGX, @BancoWF)
-- =============================================================================

USE [DadosGX_Simulacao];  -- ALTERE
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Financeiro_DePara_AgenteCobrador' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_01_Financeiro_DePara_AgenteCobrador;
GO

CREATE PROCEDURE dbo.up_01_Financeiro_DePara_AgenteCobrador
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
    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.AgenteCobrador_DePara
        (agc_cd, agc_nm, AgenteCobrador_Codigo, AgenteCobrador_Descricao, Origem)
    SELECT DISTINCT
        agc_cd = ISNULL(a.AGENTECOBRADOR_CODIGO, ''''),
        agc_nm = ISNULL(a.AGENTECOBRADOR_DESCRICAO, ''''),
        AgenteCobrador_Codigo = ''S/DePara'',
        AgenteCobrador_Descricao = ''S/DePara'',
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
            FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.AgenteCobrador_DePara b
            WHERE ISNULL(a.AGENTECOBRADOR_CODIGO, '''') = ISNULL(b.agc_cd, '''') COLLATE DATABASE_DEFAULT
        )

    UPDATE a
    SET
        a.AgenteCobrador_Codigo = b.AgenteCobrador_Codigo,
        a.AgenteCobrador_Descricao = b.AgenteCobrador_Descricao
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.AgenteCobrador_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.AgenteCobrador b
        ON b.AgenteCobrador_Descricao = a.agc_nm COLLATE Latin1_General_CI_AI
    WHERE a.AgenteCobrador_Codigo = ''S/DePara''
'
EXEC sp_executesql @CMD
GO
