-- =============================================================================
-- Layout: Fseg_Srv
-- Staging : Arquivo_FSeg_Srv_Tratado
-- Destino : Ficha_Srv_MG
-- Procedure: up_01_Extrai_Fseg_Srv_gx (@BancoDadosGX, @BancoWF)
-- Depende : Ficha_Cab_MG (layout 13 Fseg_Cab)
-- =============================================================================

USE [DadosGX_Simulacao];  -- ALTERE
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Extrai_Fseg_Srv_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_01_Extrai_Fseg_Srv_gx;
GO

CREATE PROCEDURE dbo.up_01_Extrai_Fseg_Srv_gx
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
        WHERE type = ''U'' AND name = ''Arquivo_FSeg_Srv_Tratado''
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

PRINT '=========================================================================================='
PRINT ' CRIA a cópia do Arquivo_FSeg_Srv_Tratado para Migração'
PRINT '=========================================================================================='

SELECT @CMD = '
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects WHERE type = ''U'' AND name =''Ficha_Srv_MG'') )
    BEGIN
        SELECT a.* INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG
        FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Arquivo_FSeg_Srv_Tratado a
        WHERE 1 = 1
    END
'
EXEC sp_executesql @CMD

SELECT @CMD = '
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''Veiculo_Codigo'' AND obj.name = ''Ficha_Srv_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG ADD Veiculo_Codigo int NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''TMO_CodigoWF'' AND obj.name = ''Ficha_Srv_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG ADD TMO_CodigoWF int NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''TMO_DescricaoWF'' AND obj.name = ''Ficha_Srv_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG ADD TMO_DescricaoWF varchar(50) NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''Empresa_Codigo'' AND obj.name = ''Ficha_Srv_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG ADD Empresa_Codigo int NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''TipoOSCod'' AND obj.name = ''Ficha_Srv_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG ADD TipoOSCod int NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''Ocorrencia'' AND obj.name = ''Ficha_Srv_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG ADD Ocorrencia varchar(500) NULL
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' ATUALIZA Flag=0 na Tabela Ficha_Srv_MG CHASSI em BRANCO/NULO'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a SET a.Ocorrencia = '''' FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG a

    UPDATE a
    SET a.Ocorrencia = CASE
                        WHEN CHASSI IS NULL OR CHASSI = ''''
                        THEN a.Ocorrencia + ''CHASSI é BRANCO/NULO.''
                        ELSE a.Ocorrencia
                    END,
        a.Flag = CASE
                    WHEN CHASSI IS NULL OR CHASSI = '''' THEN 0
                    ELSE a.Flag
                END
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG a
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' ATUALIZA Flag=0 na Tabela Ficha_Srv_MG NUMERO_OS em BRANCO/NULO'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.Ocorrencia = CASE
                        WHEN NUMERO_OS IS NULL OR NUMERO_OS = ''''
                        THEN a.Ocorrencia + '' | NUMERO_OS é BRANCO/NULO.''
                        ELSE a.Ocorrencia
                    END,
        a.Flag = CASE
                    WHEN NUMERO_OS IS NULL OR NUMERO_OS = '''' THEN 0
                    ELSE a.Flag
                END
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG a
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' ATUALIZA Flag=0 na Tabela Ficha_Srv_MG TMO_REFERENCIA em BRANCO/NULO'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.Ocorrencia = CASE
                        WHEN TMO_REFERENCIA IS NULL OR TMO_REFERENCIA = ''''
                        THEN a.Ocorrencia + '' | TMO_REFERENCIA é BRANCO/NULO.''
                        ELSE a.Ocorrencia
                    END,
        a.Flag = CASE
                    WHEN TMO_REFERENCIA IS NULL OR TMO_REFERENCIA = '''' THEN 0
                    ELSE a.Flag
                END
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG a
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' ATUALIZA Flag=0 na Tabela Ficha_Srv_MG Chassi NÃO EXISTE na Ficha_Cab_MG'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.Flag = 0
        ,a.Ocorrencia = a.Ocorrencia + '' | Chassi NÃO EXISTE na Ficha_Cab_MG.''
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG a
    WHERE
        NOT EXISTS(
            SELECT 1
            FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG b
            WHERE
                a.CHASSI = b.CHASSI
        )
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' ATUALIZA Flag=0 na Tabela Ficha_Srv_MG TMO_QUANTIDADE = 0'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.Flag = 0
        ,a.Ocorrencia = a.Ocorrencia + '' | TMO_QUANTIDADE = 0.''
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG a
    WHERE
        a.Flag = 1 AND
        (TMO_QUANTIDADE IS NULL OR TMO_QUANTIDADE = ''''
        OR TRY_CAST(RTRIM(LTRIM(TMO_QUANTIDADE)) AS DECIMAL(18, 6)) = 0)
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' ATUALIZA Flag=0 na Tabela Ficha_Srv_MG devido o Flag = 0 na Ficha_Cab_MG'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.Flag = 0
        ,a.Ocorrencia = a.Ocorrencia + '' | Registro é FLAG=0 na Ficha_Cab_MG.''
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG a
    WHERE
        EXISTS(
            SELECT 1
            FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG b
            WHERE
                a.CHASSI = b.CHASSI AND
                a.NUMERO_OS = b.NUMERO_OS AND
                b.Flag = 0
        )
'
EXEC sp_executesql @CMD

/* VERIFICAR COM MAIS CALMA — duplicidade comentada no legado */


PRINT '=========================================================================================='
PRINT ' Ajusta campo CHASSI na Tabela Ficha_Srv_MG'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET
        a.CHASSI = UPPER(dbo.fn_Remove_Caracteres_Especiais(a.CHASSI))
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG a
    WHERE
        a.Flag = 1
'
EXEC sp_executesql @CMD


PRINT '========================================================================================='
PRINT ' ATUALIZA Ocorrencia na Ficha_Srv_MG para LEN(CHASSI)>17 OR LEN(CHASSI)<17'
PRINT '========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.Ocorrencia = a.Ocorrencia + '' | QTDE de Caracteres no CHASSI é INVÁLIDA.''
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG a
    WHERE
        a.Flag = 1 AND
        (LEN(a.CHASSI) > 17 OR LEN(CHASSI) < 17)
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' AJUSTE campo NUMERO_OS na Tabela Ficha_Srv_MG'
PRINT '=========================================================================================='

SELECT @CMD = '
    IF( SELECT COUNT(*) FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG WHERE ISNUMERIC(NUMERO_OS) = 0 ) > 0
    BEGIN
        UPDATE a
        SET a.NUMERO_OS = CASE
                            WHEN dbo.FN_RemoveCaracteresNaoInteiros(a.NUMERO_OS) IS NULL OR dbo.FN_RemoveCaracteresNaoInteiros(a.NUMERO_OS) = 0 THEN 1
                            ELSE dbo.FN_RemoveCaracteresNaoInteiros(a.NUMERO_OS)
                        END
        FROM
            ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG a
        WHERE
            a.Flag = 1
    END

    ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG ALTER COLUMN NUMERO_OS int;
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' ATUALIZA Veiculo_Codigo na tabela Ficha_Srv_MG'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a SET
        a.Veiculo_Codigo = b.Veiculo_Codigo
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Veiculo b ON a.CHASSI = b.Veiculo_Chassi COLLATE DATABASE_DEFAULT
    WHERE
        a.Flag = 1
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' ATUALIZA Empresa na tabela Ficha_Srv_MG'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.Empresa_Codigo = b.Empresa_Codigo
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG a
    INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Empresa_DePara b on b.Pessoa_DocIdentificador = a.CNPJ_EMPRESA collate database_default
    WHERE
        a.Flag = 1
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' ATUALIZA Flag=0 na Tabela Ficha_Srv_MG Empresa_Codigo em VAZIO/NULO'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.Flag = 0
        ,a.Ocorrencia = a.Ocorrencia + '' | Empresa_Codigo é VAZIO/NULO.''
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG a
    WHERE
        a.Flag = 1 AND
        a.Empresa_Codigo IS NULL
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' ATUALIZA TMO_REFERENCIA na tabela Ficha_Srv_MG'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.TMO_CodigoWF = TRY_CAST(RTRIM(LTRIM(a.TMO_REFERENCIA)) AS int)
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG a
    WHERE
        a.Flag = 1

    UPDATE a
    SET a.TMO_DescricaoWF = (SUBSTRING(a.TMO_DESCRICAO,1,50))
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG a
    WHERE
        a.Flag = 1
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' AJUSTE de colunas VALOR com "","" na Tabela Ficha_Srv_MG'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.TMO_QUANTIDADE = REPLACE(a.TMO_QUANTIDADE,'','',''.'')
        ,a.VALOR_UNITARIO = REPLACE(a.VALOR_UNITARIO,'','',''.'')
        ,a.VALOR_DESCONTO = REPLACE(a.VALOR_DESCONTO,'','',''.'')
        ,a.VALOR_TOTAL = REPLACE(a.VALOR_TOTAL,'','',''.'')
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG a

    ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG ALTER COLUMN TMO_QUANTIDADE FLOAT
    ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG ALTER COLUMN VALOR_UNITARIO FLOAT
    ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG ALTER COLUMN VALOR_DESCONTO FLOAT
    ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG ALTER COLUMN VALOR_TOTAL FLOAT
'
EXEC sp_executesql @CMD


PRINT '========================================================================================='
PRINT ' ATUALIZA Ocorrencia na Ficha_Srv_MG para VEÍCULOS SEM CADASTRO no WF'
PRINT '========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.Ocorrencia = a.Ocorrencia + '' | CADASTRAR VEÍCULO no WF.''
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG a
    WHERE
        a.Flag = 1 AND
        a.Veiculo_Codigo IS NULL
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' AJUSTE de colunas TMO_COBRADO na Tabela Ficha_Srv_MG'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.TMO_COBRADO = CASE
                            WHEN a.TMO_COBRADO IS NULL OR a.TMO_COBRADO = '''' THEN 1
                            WHEN a.TMO_COBRADO = ''S'' THEN 1
                            WHEN a.TMO_COBRADO = ''0'' OR a.TMO_COBRADO = 0 THEN 0
                            ELSE TRY_CAST(a.TMO_COBRADO AS int)
                        END
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG a

    ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG ALTER COLUMN TMO_COBRADO INT
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' VERIFICANDO CRITICAS DE EXTRAÇÃO DA TABELA Ficha_Srv_MG'
PRINT '=========================================================================================='

SELECT @CMD = '
    IF ( SELECT COUNT(*) FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG a WHERE a.Ocorrencia != '''' ) > 0
    BEGIN
        PRINT '' ENCONTROU CRITICAS DE EXTRAÇÃO DA TABELA <Ficha_Srv_MG>''

        SELECT
            ''Ficha_Srv_MG'' AS [TABELA],
            COUNT(*) AS [QTDE DE REGISTROS FLAG = 0 ]
        FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG a
        WHERE
            a.Flag = 0

        SELECT
            ''Ficha_Srv_MG'' AS [TABELA],
            COUNT(*) AS [QTDE DE REGISTROS COM OCORRÊNCIAS]
        FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG a
        WHERE
            a.Ocorrencia != ''''
    END
    ELSE
        PRINT '' NÃO ENCONTROU CRITICAS DE EXTRAÇÃO DA TABELA <Ficha_Srv_MG>''
'
EXEC sp_executesql @CMD
GO
