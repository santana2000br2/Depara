-- =============================================================================
-- Layout: 7 Produto De/Para
-- Tabela : TabelaPreco_DePara
-- Procedure: up_06_Produto_DePara_TabelaPreco (@BancoDadosGX, @BancoWF)
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================

USE [DadosGX_SeuProjeto];  -- ALTERE para o nome real do banco DadosGX
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_06_Produto_DePara_TabelaPreco' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_06_Produto_DePara_TabelaPreco;
GO

CREATE PROCEDURE dbo.up_06_Produto_DePara_TabelaPreco
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF (NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX))
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

SELECT @CMD = '

PRINT ''=========================================================================================='' 
PRINT '' 01 - GERA TabelaPreco_DePara '' 
PRINT ''==========================================================================================''

    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TabelaPreco_DePara
        (Empresa_Codigo, Empresa_NomeFantasia, EmpresaTabelaPreco_TabPrecoCod, EmpresaTabelaPreco_TabelaPrecoTipo, TabelaPreco_Codigo, TabelaPreco_Descricao, TabelaPreco_Tipo, banco_principal)
    SELECT DISTINCT
        Empresa_Codigo = a.Empresa_Codigo,
        Empresa_NomeFantasia = b.Empresa_NomeFantasia,
        EmpresaTabelaPreco_TabPrecoCod = a.EmpresaTabelaPreco_TabPrecoCod,
        EmpresaTabelaPreco_TabelaPrecoTipo = a.EmpresaTabelaPreco_TabelaPrecoTipo,
        TabelaPreco_Codigo = c.TabelaPreco_Codigo,
        TabelaPreco_Descricao = c.TabelaPreco_Descricao,
        TabelaPreco_Tipo = c.TabelaPreco_Tipo,
        banco_principal = b.Empresa_MarcaCod
    FROM ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.EmpresaTabelaPreco a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Empresa b ON (a.Empresa_Codigo = b.Empresa_Codigo)
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.TabelaPreco c ON (
        a.EmpresaTabelaPreco_TabPrecoCod = c.TabelaPreco_Codigo AND
        a.EmpresaTabelaPreco_TabelaPrecoTipo = c.TabelaPreco_Tipo
    )
    INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Empresa_DePara d ON (b.Empresa_Codigo = d.Empresa_Codigo)
    WHERE
        NOT EXISTS (
            SELECT 1
            FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TabelaPreco_DePara e
            WHERE
                e.Empresa_Codigo = a.Empresa_Codigo AND
                e.EmpresaTabelaPreco_TabPrecoCod = a.EmpresaTabelaPreco_TabPrecoCod
        )
'
EXEC sp_executesql @CMD
GO
