-- =============================================================================
-- Layout: 14 Fseg_Prd
-- Staging : Arquivo_FSeg_Prd_Tratado
-- Destino : Ficha_Prd_MG
-- Procedure: up_01_Extrai_Fseg_Prd_gx (@BancoDadosGX, @BancoWF)
-- Depende : Ficha_Cab_MG (layout 13 Fseg_Cab)
-- =============================================================================

USE [DadosGX_Simulacao];  -- ALTERE
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Extrai_Fseg_Prd_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_01_Extrai_Fseg_Prd_gx;
GO

CREATE PROCEDURE dbo.up_01_Extrai_Fseg_Prd_gx
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
        WHERE type = ''U'' AND name = ''Arquivo_FSeg_Prd_Tratado''
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
PRINT ' Cria a cópia do Arquivo_FSeg_Prd_Tratado para Migração'
PRINT '=========================================================================================='

SELECT @CMD = '
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects WHERE type = ''U'' AND name =''Ficha_Prd_MG'') )
    BEGIN
        SELECT a.* INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG
        FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Arquivo_FSeg_Prd_Tratado a
        WHERE 1 = 1
    END
'
EXEC sp_executesql @CMD

SELECT @CMD = '
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''Veiculo_Codigo'' AND obj.name = ''Ficha_Prd_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG ADD Veiculo_Codigo int NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''Produto_Codigo'' AND obj.name = ''Ficha_Prd_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG ADD Produto_Codigo int NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''ProdutoMarca_MarcaCod'' AND obj.name = ''Ficha_Prd_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG ADD ProdutoMarca_MarcaCod int NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''PRODUTO_REFERENCIA_Ajustado'' AND obj.name = ''Ficha_Prd_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG ADD PRODUTO_REFERENCIA_Ajustado nvarchar(510) NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''PRODUTO_REFERENCIATRANS'' AND obj.name = ''Ficha_Prd_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG ADD PRODUTO_REFERENCIATRANS nvarchar(510) NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''Empresa_Codigo'' AND obj.name = ''Ficha_Prd_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG ADD Empresa_Codigo int NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''TipoOSCod'' AND obj.name = ''Ficha_Prd_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG ADD TipoOSCod int NULL
    IF ( NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns col INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects obj ON col.object_id = obj.object_id WHERE col.name = ''Ocorrencia'' AND obj.name = ''Ficha_Prd_MG''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG ADD Ocorrencia varchar(500) NULL
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' Atualiza Flag=0 na Tabela Ficha_Prd_MG CHASSI em BRANCO/NULO'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a SET a.Ocorrencia = '''' FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG a

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
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG a
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' Atualiza Flag=0 na Tabela Ficha_Prd_MG PRODUTO_REFERENCIA em BRANCO/NULO'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.Ocorrencia = CASE
                        WHEN PRODUTO_REFERENCIA IS NULL OR PRODUTO_REFERENCIA = ''''
                        THEN a.Ocorrencia + '' | PRODUTO_REFERENCIA é BRANCO/NULO.''
                        ELSE a.Ocorrencia
                    END,
        a.Flag = CASE
                    WHEN PRODUTO_REFERENCIA IS NULL OR PRODUTO_REFERENCIA = '''' THEN 0
                    ELSE a.Flag
                END
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG a
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' Atualiza Flag=0 na Tabela Ficha_Prd_MG NUMERO_OS em BRANCO/NULO'
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
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG a
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' Atualiza Flag=0 na Tabela Ficha_Prd_MG Chassi NÃO EXISTE na Ficha_Cab_MG'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.Flag = 0
        ,a.Ocorrencia = a.Ocorrencia + '' | Chassi NÃO EXISTE na Ficha_Cab_MG.''
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG a
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
PRINT ' Atualiza Flag=0 na Tabela Ficha_Prd_MG PRODUTO_QUANTIDADE = 0'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.Flag = 0
        ,a.Ocorrencia = a.Ocorrencia + '' | PRODUTO_QUANTIDADE = 0.''
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG a
    WHERE
        a.Flag = 1 AND
        (PRODUTO_QUANTIDADE IS NULL OR PRODUTO_QUANTIDADE = ''''
        OR TRY_CAST(RTRIM(LTRIM(PRODUTO_QUANTIDADE)) AS DECIMAL(18, 6)) = 0)
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' Atualiza Flag=0 na Tabela Ficha_Prd_MG devido o Flag = 0 na Ficha_Cab_MG'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.Flag = 0
        ,a.Ocorrencia = a.Ocorrencia + '' | Registro é FLAG=0 na Ficha_Cab_MG.''
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG a
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

/* VERIFICAR COM MAIS CALMA — duplicidade comentada no legado
PRINT '=========================================================================================='
PRINT ' Remove as duplicidades da Ficha_Prd_MG'
PRINT '=========================================================================================='
*/


PRINT '=========================================================================================='
PRINT ' Ajusta campo CHASSI na Tabela Ficha_Prd_MG'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET
        a.CHASSI = UPPER(dbo.fn_Remove_Caracteres_Especiais(a.CHASSI))
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG a
    WHERE
        a.Flag = 1
'
EXEC sp_executesql @CMD


PRINT '========================================================================================='
PRINT ' Atualiza Ocorrencia na Ficha_Prd_MG para LEN(CHASSI)>17 OR LEN(CHASSI)<17'
PRINT '========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.Ocorrencia = a.Ocorrencia + '' | QTDE de Caracteres no CHASSI é INVÁLIDA.''
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG a
    WHERE
        a.Flag = 1 AND
        (LEN(a.CHASSI) > 17 OR LEN(CHASSI) < 17)
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' Ajusta campo NUMERO_OS na Tabela Ficha_Prd_MG'
PRINT '=========================================================================================='

SELECT @CMD = '
    IF( SELECT COUNT(*) FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG WHERE ISNUMERIC(NUMERO_OS) = 0 ) > 0
    BEGIN
        UPDATE a
        SET a.NUMERO_OS = CASE
                            WHEN dbo.FN_RemoveCaracteresNaoInteiros(a.NUMERO_OS) IS NULL OR dbo.FN_RemoveCaracteresNaoInteiros(a.NUMERO_OS) = 0 THEN 1
                            ELSE dbo.FN_RemoveCaracteresNaoInteiros(a.NUMERO_OS)
                        END
        FROM
            ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG a
        WHERE
            a.Flag = 1
    END

    ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG ALTER COLUMN NUMERO_OS int;
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' Atualiza Veiculo_Codigo na tabela Ficha_Prd_MG'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a SET
        a.Veiculo_Codigo = b.Veiculo_Codigo
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.Veiculo b ON a.CHASSI = b.Veiculo_Chassi COLLATE DATABASE_DEFAULT
    WHERE
        a.Flag = 1
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' Atualiza Marca e Empresa na tabela Ficha_Prd_MG'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.ProdutoMarca_MarcaCod = b.Empresa_MarcaCod
        ,a.Empresa_Codigo = b.Empresa_Codigo
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG a
    INNER JOIN ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Empresa_DePara b on b.Pessoa_DocIdentificador = a.CNPJ_EMPRESA collate database_default
    WHERE
        a.Flag = 1
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' Atualiza Flag=0 na Tabela Ficha_Prd_MG Empresa_Codigo em VAZIO/NULO'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.Flag = 0
        ,a.Ocorrencia = a.Ocorrencia + '' | Empresa_Codigo é VAZIO/NULO.''
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG a
    WHERE
        a.Flag = 1 AND
        a.Empresa_Codigo IS NULL
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' Atualiza PRODUTO_REFERENCIA / PRODUTO_REFERENCIATRANS na tabela Ficha_Prd_MG'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.PRODUTO_REFERENCIA_Ajustado = RTRIM(LTRIM(a.PRODUTO_REFERENCIA))
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG a
    WHERE
        a.Flag = 1 AND
        a.ProdutoMarca_MarcaCod NOT IN (14,36,54) --FORD / VOLKS / MAN

    UPDATE a
    SET a.PRODUTO_REFERENCIATRANS = (dbo.fn_Remove_Caracteres_Especiais(a.PRODUTO_REFERENCIA_Ajustado))
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG a
    WHERE
        a.Flag = 1 AND
        a.ProdutoMarca_MarcaCod NOT IN (14,36,54) --FORD / VOLKS / MAN
'
EXEC sp_executesql @CMD


PRINT '============================================================================================================'
PRINT ' Atualiza PRODUTO_REFERENCIA / PRODUTO_REFERENCIATRANS das marcas FORD / VOLKS / MAN na Tabela Ficha_Prd_MG'
PRINT '============================================================================================================'

SELECT @CMD = '
    IF EXISTS ( SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG WHERE ProdutoMarca_MarcaCod IN (14,36,54) )
    BEGIN
        UPDATE a
        SET a.PRODUTO_REFERENCIA_Ajustado = CASE
                                            WHEN LEN(REPLICATE('' '',5 - LEN(SUBSTRING(a.PRODUTO_REFERENCIA,1,CHARINDEX(''/'',a.PRODUTO_REFERENCIA)))) + a.PRODUTO_REFERENCIA) > 30 THEN a.PRODUTO_REFERENCIA
                                            ELSE REPLICATE('' '',5 - LEN(SUBSTRING(a.PRODUTO_REFERENCIA,1,CHARINDEX(''/'',a.PRODUTO_REFERENCIA)))) + a.PRODUTO_REFERENCIA
                                            END
        FROM
            ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG a
        WHERE
            a.Flag = 1 AND
            a.ProdutoMarca_MarcaCod IN (14,36,54) AND
            a.PRODUTO_REFERENCIA LIKE ''%/%'' AND
            LEN( SUBSTRING( a.PRODUTO_REFERENCIA, 1, CHARINDEX(''/'', a.PRODUTO_REFERENCIA ) ) ) < 5 AND
            LEN( RTRIM( LTRIM( a.PRODUTO_REFERENCIA ) ) ) < 30

        UPDATE a
        SET a.PRODUTO_REFERENCIATRANS = UPPER(dbo.fn_Remove_Caracteres_Especiais(a.PRODUTO_REFERENCIA))
        FROM
            ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG a
        WHERE
            a.Flag = 1 AND
            a.ProdutoMarca_MarcaCod IN (14,36,54)
    END
    ELSE
        PRINT ''  NÃO EXISTEM PRODUTOS DAS MARCAS FORD / VOLKS / MAN na Tabela Ficha_Prd_MG PARA AJUSTE DE REFERENCIA. ''
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' Atualiza Produto_Codigo na Tabela Ficha_Prd_MG'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.Produto_Codigo = b.Produto_Codigo
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.ProdutoMarca b ON (
        a.ProdutoMarca_MarcaCod = b.ProdutoMarca_MarcaCod AND
        rtrim(Ltrim(a.PRODUTO_REFERENCIA_Ajustado)) = rtrim(Ltrim(b.ProdutoMarca_Referencia)) collate database_default AND
        rtrim(Ltrim(a.PRODUTO_REFERENCIATRANS)) = rtrim(Ltrim(b.ProdutoMarca_ReferenciaAlfanumerico)) collate database_default
    )
    WHERE
        a.Flag = 1
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' AJUSTE de colunas VALOR com "","" na Tabela Ficha_Prd_MG'
PRINT '=========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.PRODUTO_QUANTIDADE = REPLACE(a.PRODUTO_QUANTIDADE,'','',''.'')
        ,a.VALOR_UNITARIO = REPLACE(a.VALOR_UNITARIO,'','',''.'')
        ,a.VALOR_DESCONTO = REPLACE(a.VALOR_DESCONTO,'','',''.'')
        ,a.VALOR_TOTAL = REPLACE(a.VALOR_TOTAL,'','',''.'')
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG a

    ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG ALTER COLUMN PRODUTO_QUANTIDADE FLOAT
    ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG ALTER COLUMN VALOR_UNITARIO FLOAT
    ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG ALTER COLUMN VALOR_DESCONTO FLOAT
    ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG ALTER COLUMN VALOR_TOTAL FLOAT
'
EXEC sp_executesql @CMD


PRINT '========================================================================================='
PRINT ' Atualiza Ocorrencia na Ficha_Prd_MG para VEÍCULOS SEM CADASTRO no WF'
PRINT '========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.Ocorrencia = a.Ocorrencia + '' | CADASTRAR VEÍCULO no WF.''
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG a
    WHERE
        a.Flag = 1 AND
        a.Veiculo_Codigo IS NULL
'
EXEC sp_executesql @CMD


PRINT '========================================================================================='
PRINT ' Atualiza Ocorrencia na Ficha_Prd_MG para PRODUTOS SEM CADASTRO no WF'
PRINT '========================================================================================='

SELECT @CMD = '
    UPDATE a
    SET a.Ocorrencia = a.Ocorrencia + '' | CADASTRAR PRODUTOS no WF.''
    FROM
        ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG a
    WHERE
        a.Flag = 1 AND
        a.Produto_Codigo IS NULL
'
EXEC sp_executesql @CMD


PRINT '=========================================================================================='
PRINT ' VERIFICANDO CRITICAS DE EXTRAÇÃO DA TABELA Ficha_Prd_MG'
PRINT '=========================================================================================='

SELECT @CMD = '
    IF ( SELECT COUNT(*) FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG a WHERE a.Ocorrencia != '''' ) > 0
    BEGIN
        PRINT '' ENCONTROU CRITICAS DE EXTRAÇÃO DA TABELA <Ficha_Prd_MG>''

        SELECT
            ''Ficha_Prd_MG'' AS [TABELA],
            COUNT(*) AS [QTDE DE REGISTROS FLAG = 0 ]
        FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG a
        WHERE
            a.Flag = 0

        SELECT
            ''Ficha_Prd_MG'' AS [TABELA],
            COUNT(*) AS [QTDE DE REGISTROS COM OCORRÊNCIAS]
        FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG a
        WHERE
            a.Ocorrencia != ''''
    END
    ELSE
        PRINT '' NÃO ENCONTROU CRITICAS DE EXTRAÇÃO DA TABELA <Ficha_Prd_MG>''
'
EXEC sp_executesql @CMD
GO
