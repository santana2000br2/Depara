-- =============================================================================
-- DROP — Procedures de Extração (DadosGX)
-- Pacote DadosGX — gerar drop antes da reinstalacao
-- Gerado em: 01/10/2026 08:20
-- =============================================================================

USE [DadosGX_SeuProjeto];  -- ALTERE para o nome real do banco DadosGX
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Extrai_Adiantamento_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_01_Extrai_Adiantamento_gx];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Extrai_Financeiro_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_01_Extrai_Financeiro_gx];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Extrai_Pessoa_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_01_Extrai_Pessoa_gx];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_07_Extrai_PessoaConjuge_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_07_Extrai_PessoaConjuge_gx];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_06_Extrai_PessoaContato_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_06_Extrai_PessoaContato_gx];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_08_Extrai_PessoaBanco_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_08_Extrai_PessoaBanco_gx];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_03_Extrai_PessoaDoc_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_03_Extrai_PessoaDoc_gx];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_02_Extrai_PessoaEndereco_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_02_Extrai_PessoaEndereco_gx];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_04_Extrai_PessoaEnquadramento_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_04_Extrai_PessoaEnquadramento_gx];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_05_Extrai_PessoaTelefone_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_05_Extrai_PessoaTelefone_gx];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Extrai_Fseg_Cab_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_01_Extrai_Fseg_Cab_gx];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Extrai_Fseg_Prd_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_01_Extrai_Fseg_Prd_gx];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Extrai_Fseg_Srv_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_01_Extrai_Fseg_Srv_gx];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Extrai_MovimentoEstoque_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_01_Extrai_MovimentoEstoque_gx];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Extrai_ProdLocacao_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_01_Extrai_ProdLocacao_gx];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Extrai_Produto_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_01_Extrai_Produto_gx];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_02_Atualiza_Referencia_Produto_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_02_Atualiza_Referencia_Produto_gx];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_03_Trata_Duplicidade_Produto_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_03_Trata_Duplicidade_Produto_gx];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_04_Atualiza_Ocorrencia_Produto_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_04_Atualiza_Ocorrencia_Produto_gx];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Extrai_ProdutoEstoque_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_01_Extrai_ProdutoEstoque_gx];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Extrai_Veiculo_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_01_Extrai_Veiculo_gx];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_09_Extrai_Criticas' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_09_Extrai_Criticas];
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_Replace_Name_DadosGx_Procedures' AND TYPE = 'P')
    DROP PROCEDURE dbo.[up_Replace_Name_DadosGx_Procedures];
GO
