-- =============================================================================
-- DROP — Procedures De/Para (DadosGX)
-- Pacote DadosGX — gerar drop antes da reinstalacao
-- Gerado em: 10/08/2026 17:23
-- =============================================================================

USE [DadosGX_SeuProjeto];  -- ALTERE para o nome real do banco DadosGX
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Pessoa_DePara_SegmentoMercado' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_01_Pessoa_DePara_SegmentoMercado];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_02_Pessoa_DePara_Escolaridade' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_02_Pessoa_DePara_Escolaridade];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_03_Pessoa_DePara_Profissao' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_03_Pessoa_DePara_Profissao];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_04_Pessoa_DePara_EstadoCivil' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_04_Pessoa_DePara_EstadoCivil];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_05_Pessoa_DePara_Municipio' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_05_Pessoa_DePara_Municipio];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_06_Pessoa_DePara_TipoLogradouro' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_06_Pessoa_DePara_TipoLogradouro];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_07_Pessoa_DePara_Estado_Pais' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_07_Pessoa_DePara_Estado_Pais];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_08_Pessoa_DePara_Banco' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_08_Pessoa_DePara_Banco];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Produto_DePara_Unidade' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_01_Produto_DePara_Unidade];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_02_Produto_DePara_TipoProduto' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_02_Produto_DePara_TipoProduto];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_03_Produto_DePara_GrupoLucratividade' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_03_Produto_DePara_GrupoLucratividade];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_04_Produto_DePara_GrupoProduto' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_04_Produto_DePara_GrupoProduto];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_05_Produto_DePara_Procedencia' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_05_Produto_DePara_Procedencia];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_06_Produto_DePara_TabelaPreco' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_06_Produto_DePara_TabelaPreco];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_ProdutoEstoque_DePara_Estoque' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_01_ProdutoEstoque_DePara_Estoque];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Veiculo_DePara_ModeloVeiculo' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_01_Veiculo_DePara_ModeloVeiculo];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_02_Veiculo_DePara_CorExterna' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_02_Veiculo_DePara_CorExterna];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_03_Veiculo_DePara_CorInterna' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_03_Veiculo_DePara_CorInterna];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_04_Veiculo_DePara_VeiculoAno' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_04_Veiculo_DePara_VeiculoAno];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_05_Veiculo_DePara_Estado' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_05_Veiculo_DePara_Estado];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_06_Veiculo_DePara_Municipio' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_06_Veiculo_DePara_Municipio];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_07_Veiculo_DePara_Marca' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_07_Veiculo_DePara_Marca];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Financeiro_DePara_AgenteCobrador' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_01_Financeiro_DePara_AgenteCobrador];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_02_Financeiro_DePara_ContaGerencial' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_02_Financeiro_DePara_ContaGerencial];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_03_Financeiro_DePara_TipoTitulo' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_03_Financeiro_DePara_TipoTitulo];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_04_Financeiro_DePara_Departamento' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_04_Financeiro_DePara_Departamento];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_05_Financeiro_DePara_NaturezaOperacao' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_05_Financeiro_DePara_NaturezaOperacao];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_06_Financeiro_DePara_Banco' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_06_Financeiro_DePara_Banco];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_MovimentoEstoque_DePara_NaturezaOperacao' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_01_MovimentoEstoque_DePara_NaturezaOperacao];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_02_MovimentoEstoque_DePara_Estoque' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_02_MovimentoEstoque_DePara_Estoque];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_03_MovimentoEstoque_DePara_Departamento' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_03_MovimentoEstoque_DePara_Departamento];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Fseg_DePara_TipoOS' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_01_Fseg_DePara_TipoOS];
GO
