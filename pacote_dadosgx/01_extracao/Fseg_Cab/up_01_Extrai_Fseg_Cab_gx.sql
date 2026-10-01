-- =============================================================================
-- Layout: 13 Fseg_Cab
-- Staging : Arquivo_FSeg_Cab_Tratado
-- Destino : Ficha_Cab_MG
-- Procedure: up_01_Extrai_Fseg_Cab_gx (@BancoDadosGX, @BancoWF)
-- =============================================================================

USE [DadosGX_SeuProjeto];  -- ALTERE para o nome real do banco DadosGX
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Extrai_Fseg_Cab_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_01_Extrai_Fseg_Cab_gx;
GO

CREATE PROCEDURE dbo.up_01_Extrai_Fseg_Cab_gx
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX)
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

SELECT @CMD = N'
    IF NOT EXISTS (
        SELECT 1 FROM ' + QUOTENAME(LTRIM(RTRIM(@BancoDadosGX))) + N'.sys.objects
        WHERE type = ''U'' AND name = ''Arquivo_FSeg_Cab_Tratado''
    )
        SELECT @ok = 0
    ELSE
        SELECT @ok = 1
'
DECLARE @StagingOk BIT = 0
EXEC sp_executesql @CMD, N'@ok BIT OUTPUT', @ok = @StagingOk OUTPUT
IF @StagingOk = 0
BEGIN
    PRINT 'O < ARQUIVO > INFORMADO NAO EXISTE NESTE BANCO ' + @BancoDadosGX + '!'
    RETURN
END

SELECT @CMD = '
    UPDATE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Arquivo_FSeg_Cab_Tratado SET
        CPF_CNPJ = RTRIM(LTRIM(dbo.FN_RemoveCaracteresNaoInteiros(CPF_CNPJ)))
'
EXEC sp_executesql @CMD

SELECT @CMD = '
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects WHERE type = ''U'' AND name = ''Ficha_Cab_MG'') )
    BEGIN
        SELECT a.* INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG
        FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Arquivo_FSeg_Cab_Tratado a
        WHERE 1 = 1
    END
'
EXEC sp_executesql @CMD

SELECT @CMD = '
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''Veiculo_Codigo'' AND obj.name = ''Ficha_Cab_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG ADD Veiculo_Codigo int NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''Empresa_Codigo'' AND obj.name = ''Ficha_Cab_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG ADD Empresa_Codigo int NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''Pessoa_DocIdentificador'' AND obj.name = ''Ficha_Cab_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG ADD Pessoa_DocIdentificador varchar(20) NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''Pessoa_Codigo'' AND obj.name = ''Ficha_Cab_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG ADD Pessoa_Codigo int NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''TipoOSCod'' AND obj.name = ''Ficha_Cab_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG ADD TipoOSCod int NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''Maquina_Implemento'' AND obj.name = ''Ficha_Cab_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG ADD Maquina_Implemento int NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''Ocorrencia'' AND obj.name = ''Ficha_Cab_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG ADD Ocorrencia varchar(500) NULL
'
EXEC sp_executesql @CMD

SELECT @CMD = '
    UPDATE a SET a.Ocorrencia = '''' FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG a

    UPDATE a
    SET a.Ocorrencia = CASE WHEN CHASSI IS NULL OR CHASSI = '''' THEN a.Ocorrencia + ''CHASSI é BRANCO/NULO.'' ELSE a.Ocorrencia END,
        a.Flag = CASE WHEN CHASSI IS NULL OR CHASSI = '''' THEN 0 ELSE ISNULL(a.Flag, 1) END
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG a

    UPDATE a
    SET a.Ocorrencia = CASE WHEN CPF_CNPJ IS NULL OR CPF_CNPJ = '''' THEN a.Ocorrencia + '' | CPF_CNPJ é BRANCO/NULO.'' ELSE a.Ocorrencia END,
        a.Flag = CASE WHEN CPF_CNPJ IS NULL OR CPF_CNPJ = '''' THEN 0 ELSE a.Flag END
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG a

    UPDATE a
    SET a.Ocorrencia = CASE WHEN NUMERO_OS IS NULL OR NUMERO_OS = '''' THEN a.Ocorrencia + '' | NUMERO_OS é BRANCO/NULO.'' ELSE a.Ocorrencia END,
        a.Flag = CASE WHEN NUMERO_OS IS NULL OR NUMERO_OS = '''' THEN 0 ELSE a.Flag END
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG a
'
EXEC sp_executesql @CMD

SELECT @CMD = '
    IF(EXISTS(SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects WHERE name = ''Duplicidade_Ficha_Cab'' AND type = ''U''))
        DROP TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Duplicidade_Ficha_Cab

    SELECT a.CHASSI, a.NUMERO_OS, COUNT(a.CHASSI) AS QUANTIDADE, MAX(a.IDTabela) AS ID
    INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Duplicidade_Ficha_Cab
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG a
    GROUP BY a.CHASSI, a.NUMERO_OS, a.TIPO_OS_CODIGO, a.DATA_ABERTURA, a.DATA_LIBERACAO, a.CNPJ_EMPRESA
    HAVING COUNT(a.CHASSI) > 1

    UPDATE a
    SET a.Flag = 0,
        a.Ocorrencia = a.Ocorrencia + '' | Registros Duplicados.''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG a
    INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Duplicidade_Ficha_Cab b
        ON a.CHASSI = b.CHASSI COLLATE DATABASE_DEFAULT
       AND a.NUMERO_OS = b.NUMERO_OS COLLATE DATABASE_DEFAULT
    WHERE a.IDtabela < b.ID

    UPDATE a
    SET a.CHASSI = UPPER(dbo.fn_Remove_Caracteres_Especiais(a.CHASSI))
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG a
    WHERE a.Flag = 1

    UPDATE a
    SET a.Ocorrencia = a.Ocorrencia + '' | QTDE de Caracteres no CHASSI é INVÁLIDA.''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG a
    WHERE a.Flag = 1 AND (LEN(a.CHASSI) > 17 OR LEN(a.CHASSI) < 17)

    UPDATE a
    SET a.Pessoa_DocIdentificador = CASE
            WHEN LEN(RTRIM(LTRIM(CPF_CNPJ))) < 11
                THEN REPLICATE(''0'', 11 - LEN(RTRIM(LTRIM(CPF_CNPJ)))) + RTRIM(LTRIM(CPF_CNPJ))
            WHEN LEN(RTRIM(LTRIM(CPF_CNPJ))) > 11 AND LEN(RTRIM(LTRIM(CPF_CNPJ))) < 14
                THEN REPLICATE(''0'', 14 - LEN(RTRIM(LTRIM(CPF_CNPJ)))) + RTRIM(LTRIM(CPF_CNPJ))
            ELSE RTRIM(LTRIM(CPF_CNPJ))
        END
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG a
    WHERE a.Flag = 1
'
EXEC sp_executesql @CMD

SELECT @CMD = '
    IF (SELECT COUNT(*) FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG WHERE ISNUMERIC(KM) = 0) > 0
    BEGIN
        UPDATE a
        SET a.KM = CASE
                WHEN dbo.FN_RemoveCaracteresNaoInteiros(a.KM) IS NULL OR dbo.FN_RemoveCaracteresNaoInteiros(a.KM) = 0 THEN 1
                ELSE dbo.FN_RemoveCaracteresNaoInteiros(a.KM)
            END
        FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG a
        WHERE a.Flag = 1
    END

    IF (SELECT COUNT(*) FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG WHERE ISNUMERIC(NUMERO_OS) = 0) > 0
    BEGIN
        UPDATE a
        SET a.NUMERO_OS = CASE
                WHEN dbo.FN_RemoveCaracteresNaoInteiros(a.NUMERO_OS) IS NULL OR dbo.FN_RemoveCaracteresNaoInteiros(a.NUMERO_OS) = 0 THEN 1
                ELSE dbo.FN_RemoveCaracteresNaoInteiros(a.NUMERO_OS)
            END
        FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG a
        WHERE a.Flag = 1
    END

    UPDATE a
    SET a.DATA_ABERTURA = COALESCE(CONVERT(varchar(10), TRY_CONVERT(date, REPLACE(REPLACE(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), a.DATA_ABERTURA, 23))), 10), ''-'', ''/''), ''.'', ''/''), 103), 23),CONVERT(varchar(10), TRY_CONVERT(date, REPLACE(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), a.DATA_ABERTURA, 23))), 10), ''/'', ''-''), 23), 23),CONVERT(varchar(10), TRY_CONVERT(date, LTRIM(RTRIM(CONVERT(varchar(30), a.DATA_ABERTURA, 23))), 112), 23),CASE WHEN LEN(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), a.DATA_ABERTURA, 23))), 10)) <= 8 THEN CONVERT(varchar(10), TRY_CONVERT(date, REPLACE(REPLACE(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), a.DATA_ABERTURA, 23))), 10), ''-'', ''/''), ''.'', ''/''), 3), 23) END,''1900-01-01'')
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG a
    WHERE a.Flag = 1

    UPDATE a
    SET a.DATA_LIBERACAO = COALESCE(CONVERT(varchar(10), TRY_CONVERT(date, REPLACE(REPLACE(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), a.DATA_LIBERACAO, 23))), 10), ''-'', ''/''), ''.'', ''/''), 103), 23),CONVERT(varchar(10), TRY_CONVERT(date, REPLACE(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), a.DATA_LIBERACAO, 23))), 10), ''/'', ''-''), 23), 23),CONVERT(varchar(10), TRY_CONVERT(date, LTRIM(RTRIM(CONVERT(varchar(30), a.DATA_LIBERACAO, 23))), 112), 23),CASE WHEN LEN(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), a.DATA_LIBERACAO, 23))), 10)) <= 8 THEN CONVERT(varchar(10), TRY_CONVERT(date, REPLACE(REPLACE(LEFT(LTRIM(RTRIM(CONVERT(varchar(30), a.DATA_LIBERACAO, 23))), 10), ''-'', ''/''), ''.'', ''/''), 3), 23) END,''1900-01-01'')
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG a
    WHERE a.Flag = 1

    UPDATE a SET a.Veiculo_Codigo = b.Veiculo_Codigo
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Veiculo b
        ON a.CHASSI = b.Veiculo_Chassi COLLATE DATABASE_DEFAULT
    WHERE a.Flag = 1

    UPDATE a SET a.Ocorrencia = a.Ocorrencia + '' | CADASTRAR VEÍCULO no WF.''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG a
    WHERE a.Flag = 1 AND a.Veiculo_Codigo IS NULL

    UPDATE a SET a.Pessoa_Codigo = b.Pessoa_Codigo
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Pessoa b
        ON a.Pessoa_DocIdentificador = b.Pessoa_DocIdentificador COLLATE DATABASE_DEFAULT
    WHERE a.Flag = 1

    UPDATE a SET a.Empresa_Codigo = b.Empresa_Codigo
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG a
    INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Empresa_DePara b
        ON b.Pessoa_DocIdentificador = a.CNPJ_EMPRESA
    WHERE a.Flag = 1

    UPDATE a SET a.Ocorrencia = a.Ocorrencia + '' | EMPRESA não está na Empresa_DePara.''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG a
    WHERE a.Flag = 1 AND a.Empresa_Codigo IS NULL
'
EXEC sp_executesql @CMD
GO
