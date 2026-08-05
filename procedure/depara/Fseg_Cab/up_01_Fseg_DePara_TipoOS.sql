-- =============================================================================
-- Layout: Fseg_Cab (+ Prd/Srv quando existirem) De/Para TipoOS
-- Procedure: up_01_Fseg_DePara_TipoOS (@BancoDadosGX, @BancoWF)
-- =============================================================================

USE [DadosGX_Simulacao];  -- ALTERE
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Fseg_DePara_TipoOS' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_01_Fseg_DePara_TipoOS;
GO

CREATE PROCEDURE dbo.up_01_Fseg_DePara_TipoOS
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX)
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

-- Garante colunas usadas no insert (lote próprio)
SELECT @CMD = '
    IF NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns WHERE name = ''Origem'' AND object_id = OBJECT_ID(''' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoOS_DePara''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoOS_DePara ADD Origem varchar(50) NULL
'
EXEC sp_executesql @CMD

SELECT @CMD = '
    IF EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects WHERE type = ''U'' AND name = ''Ficha_Cab_MG'')
    BEGIN
        INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoOS_DePara
            (tpos_cd, tpos_ds, tpos_ativa, TipoOS_Codigo, TipoOS_Descricao, TipoOS_Sigla, Origem)
        SELECT DISTINCT
            tpos_cd = ISNULL(a.TIPO_OS_CODIGO, ''''),
            tpos_ds = ISNULL(a.TIPO_OS_DESCRICAO, ''''),
            tpos_ativa = '''',
            TipoOS_Codigo = ''S/DePara'',
            TipoOS_Descricao = ''S/DePara'',
            TipoOS_Sigla = ''S/DePara'',
            Origem = ''Ficha_Seguimento''
        FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Cab_MG a
        WHERE NOT EXISTS (
            SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoOS_DePara b
            WHERE b.tpos_cd = a.TIPO_OS_CODIGO COLLATE DATABASE_DEFAULT
              AND b.tpos_ds = a.TIPO_OS_DESCRICAO COLLATE DATABASE_DEFAULT
        )
    END
'
EXEC sp_executesql @CMD

SELECT @CMD = '
    IF EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects WHERE type = ''U'' AND name = ''Ficha_Prd_MG'')
    BEGIN
        INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoOS_DePara
            (tpos_cd, tpos_ds, tpos_ativa, TipoOS_Codigo, TipoOS_Descricao, TipoOS_Sigla, Origem)
        SELECT DISTINCT
            tpos_cd = ISNULL(a.TIPO_OS_CODIGO, ''''),
            tpos_ds = ISNULL(a.TIPO_OS_DESCRICAO, ''''),
            tpos_ativa = '''',
            TipoOS_Codigo = ''S/DePara'',
            TipoOS_Descricao = ''S/DePara'',
            TipoOS_Sigla = ''S/DePara'',
            Origem = ''Ficha_Seguimento''
        FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Prd_MG a
        WHERE NOT EXISTS (
            SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoOS_DePara b
            WHERE b.tpos_cd = a.TIPO_OS_CODIGO COLLATE DATABASE_DEFAULT
              AND b.tpos_ds = a.TIPO_OS_DESCRICAO COLLATE DATABASE_DEFAULT
        )
    END
'
EXEC sp_executesql @CMD

SELECT @CMD = '
    IF EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects WHERE type = ''U'' AND name = ''Ficha_Srv_MG'')
    BEGIN
        INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoOS_DePara
            (tpos_cd, tpos_ds, tpos_ativa, TipoOS_Codigo, TipoOS_Descricao, TipoOS_Sigla, Origem)
        SELECT DISTINCT
            tpos_cd = ISNULL(a.TIPO_OS_CODIGO, ''''),
            tpos_ds = ISNULL(a.TIPO_OS_DESCRICAO, ''''),
            tpos_ativa = '''',
            TipoOS_Codigo = ''S/DePara'',
            TipoOS_Descricao = ''S/DePara'',
            TipoOS_Sigla = ''S/DePara'',
            Origem = ''Ficha_Seguimento''
        FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Ficha_Srv_MG a
        WHERE NOT EXISTS (
            SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoOS_DePara b
            WHERE b.tpos_cd = a.TIPO_OS_CODIGO COLLATE DATABASE_DEFAULT
              AND b.tpos_ds = a.TIPO_OS_DESCRICAO COLLATE DATABASE_DEFAULT
        )
    END
'
EXEC sp_executesql @CMD

SELECT @CMD = '
    UPDATE a
    SET a.TipoOS_Codigo = b.TipoOS_Codigo,
        a.TipoOS_Descricao = b.TipoOS_Descricao,
        a.TipoOS_Sigla = b.TipoOS_Sigla
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.TipoOS_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.TipoOS b
        ON b.TipoOS_Sigla = a.tpos_cd
       AND b.TipoOS_Descricao = a.tpos_ds COLLATE Latin1_General_CI_AI
    WHERE a.TipoOS_Codigo = ''S/DePara''
'
EXEC sp_executesql @CMD
GO
