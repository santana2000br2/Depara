-- =============================================================================
-- Layout: Veiculo
-- Staging : Arquivo_Veiculo_Tratado
-- Destino : Veiculo_MG
-- Procedure: up_01_Extrai_Veiculo_gx (@BancoDadosGX, @BancoWF, @BancoWF_Prod opcional)
-- =============================================================================

USE [DadosGX_SeuProjeto];  -- ALTERE para o nome real do banco DadosGX
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Extrai_Veiculo_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_01_Extrai_Veiculo_gx;
GO

CREATE PROCEDURE dbo.up_01_Extrai_Veiculo_gx
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX),
    @BancoWF_Prod VARCHAR(MAX) = NULL
AS
DECLARE @CMD NVARCHAR(MAX)
DECLARE @AnoAtual CHAR(2) = RIGHT(CAST(YEAR(GETDATE()) AS VARCHAR(4)), 2)

IF NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX)
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

SELECT @CMD = N'
    IF NOT EXISTS (
        SELECT 1 FROM ' + QUOTENAME(LTRIM(RTRIM(@BancoDadosGX))) + N'.sys.objects
        WHERE type = ''U'' AND name = ''Arquivo_Veiculo_Tratado''
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
PRINT ''==========================================================================================''
PRINT '' Extrai Veiculo - Atualiza CPF_CNPJ ''
PRINT ''==========================================================================================''

    UPDATE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Arquivo_Veiculo_Tratado SET
        CPF_CNPJ = RTRIM(LTRIM(dbo.FN_RemoveCaracteresNaoInteiros(CPF_CNPJ)))
'
EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' Cria a cópia do Arquivo_Veiculo_Tratado para Migração'
PRINT '=========================================================================================='

SELECT @CMD = '
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects WHERE type = ''U'' AND name = ''Veiculo_MG'') )
    BEGIN
        SELECT a.* INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG
        FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Arquivo_Veiculo_Tratado a
        WHERE 1 = 1
    END
'
EXEC sp_executesql @CMD

SELECT @CMD = '
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''Ve_FabMod'' AND obj.name = ''Veiculo_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG ADD Ve_FabMod varchar(10) NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''Marca_CodigoWF'' AND obj.name = ''Veiculo_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG ADD Marca_CodigoWF int NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''ModeloVeiculoWF'' AND obj.name = ''Veiculo_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG ADD ModeloVeiculoWF int NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''CorInternaWF'' AND obj.name = ''Veiculo_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG ADD CorInternaWF int NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''CorExternaWF'' AND obj.name = ''Veiculo_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG ADD CorExternaWF int NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''VeiculoAno'' AND obj.name = ''Veiculo_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG ADD VeiculoAno int NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''Veiculo_Status'' AND obj.name = ''Veiculo_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG ADD Veiculo_Status char(1) NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''Empresa_Codigo'' AND obj.name = ''Veiculo_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG ADD Empresa_Codigo int NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''VeiculoProprietario'' AND obj.name = ''Veiculo_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG ADD VeiculoProprietario int NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''Veiculo_PessoaCodConcessionaria'' AND obj.name = ''Veiculo_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG ADD Veiculo_PessoaCodConcessionaria int NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''Pessoa_DocIdentificador'' AND obj.name = ''Veiculo_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG ADD Pessoa_DocIdentificador varchar(20) NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''Veiculo_EstadoCod_Placa'' AND obj.name = ''Veiculo_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG ADD Veiculo_EstadoCod_Placa char(2) NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''Veiculo_MunicipioCod_Placa'' AND obj.name = ''Veiculo_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG ADD Veiculo_MunicipioCod_Placa int NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''WMI_VIN'' AND obj.name = ''Veiculo_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG ADD WMI_VIN varchar(3) NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''WMI_Fabricante'' AND obj.name = ''Veiculo_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG ADD WMI_Fabricante varchar(150) NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''Maquina_Implemento'' AND obj.name = ''Veiculo_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG ADD Maquina_Implemento int NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''Ocorrencia'' AND obj.name = ''Veiculo_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG ADD Ocorrencia varchar(500) NULL
'
EXEC sp_executesql @CMD

SELECT @CMD = '
    UPDATE a SET a.Ocorrencia = '''' FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a

    UPDATE a
    SET a.Ocorrencia = CASE
            WHEN CHASSI IS NULL OR CHASSI = '''' THEN a.Ocorrencia + ''CHASSI é BRANCO/NULO.''
            ELSE a.Ocorrencia
        END,
        a.Flag = CASE
            WHEN CHASSI IS NULL OR CHASSI = '''' THEN 0
            ELSE 1
        END
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a

    UPDATE a
    SET a.CHASSI = UPPER(dbo.fn_Remove_Caracteres_Especiais(a.CHASSI))
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
    WHERE a.Flag = 1
'
EXEC sp_executesql @CMD

IF @BancoWF_Prod IS NOT NULL AND LTRIM(RTRIM(@BancoWF_Prod)) <> ''
BEGIN
    SELECT @CMD = '
        IF(EXISTS(SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects WHERE name = ''Veiculo_EmProducao'' AND type = ''U''))
            DROP TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_EmProducao

        SELECT a.*
        INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_EmProducao
        FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
        WHERE EXISTS (
            SELECT 1 FROM ' + LTRIM(RTRIM(@BancoWF_Prod)) + '.dbo.Veiculo b
            WHERE a.CHASSI = b.Veiculo_Chassi COLLATE database_default
        )

        UPDATE a SET
            a.Flag = 0,
            a.Ocorrencia = a.Ocorrencia + '' Cadastrado em WF-Produção.''
        FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
        INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_EmProducao b ON a.CHASSI = b.CHASSI
    '
    EXEC sp_executesql @CMD
END

SELECT @CMD = '
    IF(EXISTS(SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects WHERE name = ''Chassis_Duplicados'' AND type = ''U''))
        DROP TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Chassis_Duplicados

    SELECT a.CHASSI, COUNT(a.CHASSI) AS QUANTIDADE, MIN(a.IDTabela) AS ID
    INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Chassis_Duplicados
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
    GROUP BY a.CHASSI
    HAVING COUNT(a.CHASSI) > 1

    UPDATE a
    SET a.Flag = 0,
        a.Ocorrencia = a.Ocorrencia + '' | Duplicidades de CHASSI.''
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
    INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Chassis_Duplicados b ON a.CHASSI = b.CHASSI COLLATE database_default
    WHERE a.IDtabela > b.ID

    UPDATE a
    SET a.Pessoa_DocIdentificador = CASE
            WHEN LEN(RTRIM(LTRIM(CPF_CNPJ))) < 11
                THEN REPLICATE(''0'', 11 - LEN(RTRIM(LTRIM(CPF_CNPJ)))) + RTRIM(LTRIM(CPF_CNPJ))
            WHEN LEN(RTRIM(LTRIM(CPF_CNPJ))) > 11 AND LEN(RTRIM(LTRIM(CPF_CNPJ))) < 14
                THEN REPLICATE(''0'', 14 - LEN(RTRIM(LTRIM(CPF_CNPJ)))) + RTRIM(LTRIM(CPF_CNPJ))
            ELSE RTRIM(LTRIM(CPF_CNPJ))
        END
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
    WHERE a.Flag = 1

    IF (SELECT COUNT(*) FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG WHERE ISNUMERIC(KM) = 0) > 0
    BEGIN
        UPDATE a
        SET a.KM = CASE
                WHEN dbo.FN_RemoveCaracteresNaoInteiros(a.KM) IS NULL OR dbo.FN_RemoveCaracteresNaoInteiros(a.KM) = 0 THEN 1
                ELSE dbo.FN_RemoveCaracteresNaoInteiros(a.KM)
            END
        FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
        WHERE a.Flag = 1
    END

    UPDATE a
    SET a.SERIE = CASE
            WHEN a.SERIE <> '''' OR ISNUMERIC(a.SERIE) = 1 THEN dbo.fn_Remove_Caracteres_Especiais(a.SERIE)
            WHEN a.SERIE IS NULL OR a.SERIE = '''' THEN RIGHT(RTRIM(LTRIM(UPPER(a.CHASSI))), 8)
        END
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
    WHERE a.Flag = 1

    UPDATE a
    SET a.RENAVAM = dbo.fn_Remove_Caracteres_Especiais(a.RENAVAM)
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
    WHERE a.Flag = 1 AND a.RENAVAM <> ''''

    UPDATE a
    SET a.PLACA = CASE
            WHEN dbo.fn_Remove_Caracteres_Especiais(a.PLACA) = '''' THEN NULL
            ELSE a.PLACA
        END
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
    WHERE a.Flag = 1

    UPDATE a
    SET a.Veiculo_Status = CASE
            WHEN RTRIM(LTRIM(a.VEICULO_NOVO)) = ''S'' THEN ''N''
            ELSE ISNULL(a.VEICULO_NOVO, ''U'')
        END
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
    WHERE a.Flag = 1

    UPDATE a
    SET a.Veiculo_Status = CASE
            WHEN RTRIM(LTRIM(a.PLACA)) <> '''' THEN ''U''
            ELSE ISNULL(a.PLACA, ''N'')
        END
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
    WHERE a.Flag = 1 AND a.Veiculo_Status = ''''
'
EXEC sp_executesql @CMD

SELECT @CMD = '
    IF (SELECT COUNT(*) FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG WHERE LEN(ANO_FABRICACAO) <= 2) > 0
    BEGIN
        UPDATE a
        SET a.ANO_FABRICACAO = CASE
                WHEN (a.ANO_FABRICACAO >= ''00'' AND a.ANO_FABRICACAO <= ''' + LTRIM(RTRIM(@AnoAtual)) + ''') THEN ''20'' + a.ANO_FABRICACAO
                ELSE ''19'' + a.ANO_FABRICACAO
            END
        FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
        WHERE a.Flag = 1 AND LEN(a.ANO_FABRICACAO) <= 2
    END

    IF (SELECT COUNT(*) FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG WHERE LEN(ANO_MODELO) <= 2) > 0
    BEGIN
        UPDATE a
        SET a.ANO_MODELO = CASE
                WHEN (a.ANO_MODELO >= ''00'' AND a.ANO_MODELO <= ''' + LTRIM(RTRIM(@AnoAtual)) + ''') THEN ''20'' + a.ANO_MODELO
                ELSE ''19'' + a.ANO_MODELO
            END
        FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
        WHERE a.Flag = 1 AND LEN(a.ANO_MODELO) <= 2
    END

    UPDATE a
    SET a.Ve_FabMod = ANO_FABRICACAO + ''/'' + ANO_MODELO
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
    WHERE a.Flag = 1

    UPDATE a
    SET a.Ocorrencia = CASE
            WHEN a.ANO_FABRICACAO IS NULL OR a.ANO_FABRICACAO = '''' THEN a.Ocorrencia + '' | ANO_FABRICACAO é VAZIO/NULO.''
            ELSE a.Ocorrencia
        END
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
    WHERE a.Flag = 1

    UPDATE a
    SET a.Ocorrencia = CASE
            WHEN a.ANO_MODELO IS NULL OR a.ANO_MODELO = '''' THEN a.Ocorrencia + '' | ANO_MODELO é VAZIO/NULO.''
            ELSE a.Ocorrencia
        END
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
    WHERE a.Flag = 1

    UPDATE a
    SET a.DATA_VENDA = CASE
            WHEN a.DATA_VENDA IS NULL OR REPLACE(a.DATA_VENDA, ''/'', ''-'') = '''' OR ISDATE(a.DATA_VENDA) = 0 THEN ''1900-01-01''
            ELSE REPLACE(a.DATA_VENDA, ''/'', ''-'')
        END
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
    WHERE a.Flag = 1

    UPDATE a
    SET a.Ocorrencia = CASE
            WHEN COR_EXTERNA_CODIGO = '''' OR COR_EXTERNA_DESCRICAO = '''' THEN a.Ocorrencia + '' | COR_EXTERNA é VAZIA/NULA.''
            ELSE a.Ocorrencia
        END
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a

    UPDATE a
    SET a.WMI_VIN = SUBSTRING(a.CHASSI, 1, 3)
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
    WHERE a.Flag = 1

    UPDATE a
    SET a.WMI_Fabricante = wmi.WMI_Fabricante
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
    LEFT JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.WMI_Veiculo wmi ON wmi.WMI_VIN = a.WMI_VIN
    WHERE a.Flag = 1

    UPDATE a
    SET a.Marca_CodigoWF = m.Marca_Codigo
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
    LEFT JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Marca m ON LTRIM(RTRIM(m.Marca_Descricao)) = ISNULL(LTRIM(RTRIM(a.WMI_Fabricante)), '''') COLLATE Latin1_General_CI_AI
'
EXEC sp_executesql @CMD
GO
