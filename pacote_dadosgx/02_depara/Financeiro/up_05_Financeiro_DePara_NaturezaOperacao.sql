-- =============================================================================
-- Layout: Financeiro De/Para
-- Procedure: up_05_Financeiro_DePara_NaturezaOperacao (@BancoDadosGX, @BancoWF)
-- =============================================================================

USE [DadosGX_SeuProjeto];  -- ALTERE para o nome real do banco DadosGX
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_05_Financeiro_DePara_NaturezaOperacao' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_05_Financeiro_DePara_NaturezaOperacao;
GO

CREATE PROCEDURE dbo.up_05_Financeiro_DePara_NaturezaOperacao
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
    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.NaturezaOperacao_DePara
        (me_cd, me_ds, dep_cd, Tipo, NaturezaOperacao_Codigo, NaturezaOperacao_Descricao,
         Departamento_Codigo, Procedure_Origem)
    SELECT DISTINCT
        me_cd = ISNULL(a.NATUREZAOPERACAO_CODIGO, ''''),
        me_ds = ISNULL(a.NATUREZAOPERACAO_DESCRICAO, ''''),
        dep_cd = ISNULL(a.DEPARTAMENTO_CODIGO, ''''),
        Tipo = '''',
        NaturezaOperacao_Codigo = ''S/DePara'',
        NaturezaOperacao_Descricao = ''S/DePara'',
        Departamento_Codigo = ''S/DePara'',
        Procedure_Origem = (CASE
                                WHEN (a.TIPO_MOVFINANCEIRO = ''P'') THEN ''Obrigações''
                                WHEN (a.TIPO_MOVFINANCEIRO = ''R'') THEN ''Títulos''
                                ELSE ''''
                            END)
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Titulo_MG a
    WHERE
        a.Flag = 1 AND
        NOT EXISTS (
            SELECT 1
            FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.NaturezaOperacao_DePara d
            WHERE ISNULL(d.me_cd, '''') = ISNULL(a.NATUREZAOPERACAO_CODIGO, '''') COLLATE DATABASE_DEFAULT
        )

    UPDATE a
    SET
        a.NaturezaOperacao_Codigo = b.NaturezaOperacao_Codigo,
        a.NaturezaOperacao_Descricao = b.NaturezaOperacao_Descricao
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.NaturezaOperacao_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.NaturezaOperacao b
        ON b.NaturezaOperacao_Descricao = a.me_ds COLLATE DATABASE_DEFAULT
    WHERE a.NaturezaOperacao_Codigo = ''S/DePara''

    UPDATE a
    SET a.Departamento_Codigo = b.Departamento_Codigo
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.NaturezaOperacao_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Departamento_DePara b ON b.dep_cd = a.dep_cd
    WHERE a.Departamento_Codigo = ''S/DePara''
'
EXEC sp_executesql @CMD
GO
