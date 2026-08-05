-- =============================================================================
-- Layout: Veiculo De/Para — ModeloVeiculo
-- Procedure: up_01_Veiculo_DePara_ModeloVeiculo (@BancoDadosGX, @BancoWF)
-- =============================================================================

USE [DadosGX_Simulacao];  -- ALTERE
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_01_Veiculo_DePara_ModeloVeiculo' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_01_Veiculo_DePara_ModeloVeiculo;
GO

CREATE PROCEDURE dbo.up_01_Veiculo_DePara_ModeloVeiculo
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX)
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

-- 1) Garante colunas extras em ModeloVeiculo_DePara em lote PRÓPRIO.
--    (ALTER ADD e o uso da coluna não podem estar no mesmo lote: o SQL Server
--     compila o lote inteiro antes de executar e acusaria "Invalid column name".)
SELECT @CMD = '
    IF NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns WHERE name = ''MARCA_CODIGO'' AND object_id = OBJECT_ID(''' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ModeloVeiculo_DePara''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ModeloVeiculo_DePara ADD MARCA_CODIGO nvarchar(510) NULL
'
EXEC sp_executesql @CMD

SELECT @CMD = '
    IF NOT EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.columns WHERE name = ''CODIGO_LINHA'' AND object_id = OBJECT_ID(''' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ModeloVeiculo_DePara''))
        ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ModeloVeiculo_DePara ADD CODIGO_LINHA nvarchar(510) NULL
'
EXEC sp_executesql @CMD

-- 2) Atualiza ModeloVeiculoWF (VOLKS/MAN) — lote próprio.
SELECT @CMD = '
    UPDATE a
    SET a.ModeloVeiculoWF = m.ModeloVeiculo_Codigo
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
    LEFT JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.ModeloVeiculo m ON (
        RTRIM(LTRIM(m.ModeloVeiculo_ModeloMarca)) = RTRIM(LTRIM(a.CODIGO_LINHA)) COLLATE Latin1_General_CI_AI
        AND RTRIM(LTRIM(a.CODIGO_LINHA)) <> ''''
        AND a.Marca_CodigoWF = m.ModeloVeiculo_MarcaCod
    )
    WHERE a.Marca_CodigoWF IN (36, 54)
'
EXEC sp_executesql @CMD

-- 3) Gera ModeloVeiculo_DePara — lote próprio (colunas já existem).
SELECT @CMD = '
    INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ModeloVeiculo_DePara
        (mod_cd, mod_ds, ModeloVeiculo_Codigo, ModeloVeiculo_Descricao, ModeloVeiculo_MarcaCod,
         ModeloVeiculo_ModeloMarca, molicar_cd, ModeloVeiculo_TabelaMolicar, MARCA_CODIGO, CODIGO_LINHA)
    SELECT DISTINCT
        mod_cd = ISNULL(a.MODELO_CODIGO, ''''),
        mod_ds = ISNULL(RTRIM(LTRIM(a.MODELO_DESCRICAO)), ''''),
        ModeloVeiculo_Codigo = ''S/DePara'',
        ModeloVeiculo_Descricao = ''S/DePara'',
        ModeloVeiculo_MarcaCod = ISNULL(CAST(a.Marca_CodigoWF AS varchar), ''S/DePara''),
        ModeloVeiculo_ModeloMarca = ''S/DePara'',
        molicar_cd = '''',
        ModeloVeiculo_TabelaMolicar = '''',
        MARCA_CODIGO = ISNULL(a.VEICULO_MARCA_CODIGO, ''''),
        CODIGO_LINHA = ISNULL(a.CODIGO_LINHA, '''')
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Veiculo_MG a
    WHERE a.Flag = 1
        AND a.ModeloVeiculoWF IS NULL
        AND NOT EXISTS (
            SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ModeloVeiculo_DePara b
            WHERE ISNULL(a.VEICULO_MARCA_CODIGO, '''') = b.MARCA_CODIGO COLLATE database_default
              AND ISNULL(a.MODELO_CODIGO, '''') = b.mod_cd COLLATE database_default
              AND ISNULL(a.CODIGO_LINHA, '''') = b.CODIGO_LINHA COLLATE database_default
        )
'
EXEC sp_executesql @CMD

-- 4) Aplica De/Para por descrição e marca — lote próprio.
SELECT @CMD = '
    UPDATE a
    SET a.ModeloVeiculo_Codigo = b.ModeloVeiculo_Codigo,
        a.ModeloVeiculo_Descricao = b.ModeloVeiculo_Descricao,
        a.ModeloVeiculo_ModeloMarca = b.ModeloVeiculo_ModeloMarca
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.ModeloVeiculo_DePara a
    INNER JOIN ' + LTRIM(RTRIM(@BancoWF)) + '.dbo.ModeloVeiculo b ON (
        RTRIM(LTRIM(b.ModeloVeiculo_Descricao)) = RTRIM(LTRIM(a.mod_ds)) COLLATE Latin1_General_CI_AI
        AND a.ModeloVeiculo_MarcaCod = b.ModeloVeiculo_MarcaCod
    )
    WHERE ISNUMERIC(a.ModeloVeiculo_Codigo) = 0
      AND ISNUMERIC(a.ModeloVeiculo_MarcaCod) = 1
'
EXEC sp_executesql @CMD
GO
