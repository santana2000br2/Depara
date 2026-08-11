-- =============================================================================
-- Layout: 2 Forn_cli_Documento.txt
-- Staging : Arquivo_Forn_Cli_Documento_Tratado
-- Destino : PessoaDocumento_MG
-- Procedure: up_03_Extrai_PessoaDoc_gx (@BancoDadosGX)
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================

USE [DadosGX_SeuProjeto];  -- ALTERE para o nome real do banco DadosGX
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_03_Extrai_PessoaDoc_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_03_Extrai_PessoaDoc_gx;
GO

CREATE PROCEDURE dbo.up_03_Extrai_PessoaDoc_gx
    @BancoDadosGX VARCHAR(MAX)
AS

DECLARE @CMD NVARCHAR(MAX)

    IF ( NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX) )
        BEGIN
            PRINT 'O < '+ @BancoDadosGX +' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
            RETURN
        END
    SELECT @CMD = N'
        IF NOT EXISTS (
            SELECT 1 FROM ' + QUOTENAME(LTRIM(RTRIM(@BancoDadosGX))) + N'.sys.objects
            WHERE type = ''U'' AND name = ''Arquivo_Forn_Cli_Documento_Tratado''
        )
            SELECT @ok = 0
        ELSE
            SELECT @ok = 1
    '
    DECLARE @StagingOk BIT = 0
    EXEC sp_executesql @CMD, N'@ok BIT OUTPUT', @ok = @StagingOk OUTPUT
    IF @StagingOk = 0
        BEGIN
            PRINT 'O < ARQUIVO > INFORMADO NAO EXISTE NO BANCO '+ @BancoDadosGX +'!'
            RETURN
        END

PRINT '=========================================================================================='
PRINT ' Extrai PessoaDocumento - Atualiza CPF_CNPJ '
PRINT '=========================================================================================='

SELECT @CMD = '

    UPDATE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Arquivo_Forn_Cli_Documento_Tratado SET
        CPF_CNPJ = RTRIM(LTRIM(' + QUOTENAME(LTRIM(RTRIM(@BancoDadosGX))) + '.dbo.fn_RemoveCaracteresNaoInteiros(CPF_CNPJ)))

'

EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' Cria a cópia do Arquivo_Forn_Cli_Documento_Tratado para Migração'
PRINT '=========================================================================================='

SELECT @CMD = '

    IF(NOT EXISTS(SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects WHERE type = ''U'' AND name =''PessoaDocumento_MG''))
    BEGIN
        SELECT a.* INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaDocumento_MG FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Arquivo_Forn_Cli_Documento_Tratado a WHERE 1=1
    END

'

EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' Adicionando colunas na tabela PessoaDocumento_MG '
PRINT '=========================================================================================='

SELECT @CMD = '

    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col, ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj
                    WHERE col.object_id = obj.object_id AND col.name = ''Pessoa_DocIdentificador'' AND obj.name = ''PessoaDocumento_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaDocumento_MG ADD Pessoa_DocIdentificador varchar(20) NULL

    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col, ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj
                    WHERE col.object_id = obj.object_id AND col.name = ''Ocorrencia'' AND obj.name = ''PessoaDocumento_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaDocumento_MG ADD Ocorrencia VARCHAR(500)

'

EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' Atualiza Flag = 0 na PessoaDocumento_MG CPF/CNPJ em BRANCO '
PRINT '=========================================================================================='

SELECT @CMD = '

    UPDATE a SET a.Ocorrencia = '''' FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaDocumento_MG a

    UPDATE a
    SET    a.Ocorrencia = CASE
                        WHEN CPF_CNPJ IS NULL OR CPF_CNPJ = ''''
                        THEN a.Ocorrencia + '' | CPF CNPJ está vazio''
                        ELSE a.Ocorrencia
                    END,
        a.Flag = CASE
                    WHEN CPF_CNPJ IS NULL OR CPF_CNPJ = '''' THEN 0
                    ELSE 1
                END
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaDocumento_MG a

'

EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' Atualiza Flag = 0 Pessoa já cadastrado em WF-Produção '
PRINT '=========================================================================================='

SELECT @CMD = '

    UPDATE a SET
        a.Flag = 0
        ,a.Ocorrencia = a.Ocorrencia + '' Cadastrado da Pessoa no WF-Produção.''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaDocumento_MG a
    INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_EmProducao b ON a.CPF_CNPJ = b.CPF_CNPJ

'

EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' Atualiza Flag=0 na Tabela PessoaDocumento_MG CPF/CNPJ não existe na Pessoa_MG'
PRINT '=========================================================================================='

SELECT @CMD = '

    UPDATE a SET
        a.Flag = 0
        ,a.Ocorrencia = a.Ocorrencia + '' | CPF CNPJ não existe na Pessoa_MG.''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaDocumento_MG a
    WHERE
        1=1 AND
        NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Pessoa_MG b WHERE a.CPF_CNPJ = b.CPF_CNPJ)

'

EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' Ajusta Pessoa_DocIdentificador na Tabela PessoaDocumento_MG '
PRINT '=========================================================================================='

SELECT @CMD = '

    UPDATE a
    SET    a.Pessoa_DocIdentificador = CASE
                                WHEN len(RTRIM(LTRIM(CPF_CNPJ))) < 11
                                    THEN replicate(''0'',11 - len(RTRIM(LTRIM(CPF_CNPJ)))) + RTRIM(LTRIM(CPF_CNPJ))
                                WHEN len(RTRIM(LTRIM(CPF_CNPJ))) > 11 and len(RTRIM(LTRIM(CPF_CNPJ))) < 14
                                    THEN replicate(''0'',14 - len(RTRIM(LTRIM(CPF_CNPJ)))) + RTRIM(LTRIM(CPF_CNPJ))
                                ELSE RTRIM(LTRIM(CPF_CNPJ))
                            END
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.PessoaDocumento_MG a
    WHERE
        a.Flag = 1
'

EXEC sp_executesql @CMD

GO
