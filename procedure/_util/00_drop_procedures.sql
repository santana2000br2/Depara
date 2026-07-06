-- Remove procedures de extração (executar antes de reinstalar)
-- USE [DadosGX_SeuProjeto];  -- ALTERE
-- GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_Replace_Name_DadosGx_Procedures' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_Replace_Name_DadosGx_Procedures;
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Extrai_Pessoa_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_01_Extrai_Pessoa_gx;
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_02_Extrai_PessoaEndereco_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_02_Extrai_PessoaEndereco_gx;
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_03_Extrai_PessoaDoc_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_03_Extrai_PessoaDoc_gx;
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_04_Extrai_PessoaEnquadramento_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_04_Extrai_PessoaEnquadramento_gx;
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_05_Extrai_PessoaTelefone_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_05_Extrai_PessoaTelefone_gx;
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_06_Extrai_PessoaContato_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_06_Extrai_PessoaContato_gx;
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_07_Extrai_PessoaConjuge_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_07_Extrai_PessoaConjuge_gx;
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_08_Extrai_PessoaBanco_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_08_Extrai_PessoaBanco_gx;
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_09_Extrai_Criticas' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_09_Extrai_Criticas;
GO
