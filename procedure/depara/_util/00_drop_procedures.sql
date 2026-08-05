-- =============================================================================
-- Script SQL — Depara
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================

-- Remove procedures De/Para Pessoa (up_01 a up_08)

-- USE [DadosGX_SeuProjeto];  -- ALTERE

-- GO



IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Pessoa_DePara_SegmentoMercado' AND TYPE = 'P')

    DROP PROCEDURE dbo.up_01_Pessoa_DePara_SegmentoMercado;

GO



IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_02_Pessoa_DePara_Escolaridade' AND TYPE = 'P')

    DROP PROCEDURE dbo.up_02_Pessoa_DePara_Escolaridade;

GO



IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_03_Pessoa_DePara_Profissao' AND TYPE = 'P')

    DROP PROCEDURE dbo.up_03_Pessoa_DePara_Profissao;

GO



IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_04_Pessoa_DePara_EstadoCivil' AND TYPE = 'P')

    DROP PROCEDURE dbo.up_04_Pessoa_DePara_EstadoCivil;

GO



IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_05_Pessoa_DePara_Municipio' AND TYPE = 'P')

    DROP PROCEDURE dbo.up_05_Pessoa_DePara_Municipio;

GO



IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_06_Pessoa_DePara_TipoLogradouro' AND TYPE = 'P')

    DROP PROCEDURE dbo.up_06_Pessoa_DePara_TipoLogradouro;

GO



IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_07_Pessoa_DePara_Estado_Pais' AND TYPE = 'P')

    DROP PROCEDURE dbo.up_07_Pessoa_DePara_Estado_Pais;

GO



IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_08_Pessoa_DePara_Banco' AND TYPE = 'P')

    DROP PROCEDURE dbo.up_08_Pessoa_DePara_Banco;

GO

