-- =============================================================================
-- Layout: 7 Produto
-- Staging : Produto_MG
-- Destino : Produto_MG
-- Procedure: up_04_Atualiza_Ocorrencia_Produto_gx (@BancoDadosGX, @BancoWF)
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================

USE [DadosGX_Simulacao];  -- ALTERE
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_04_Atualiza_Ocorrencia_Produto_gx' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_04_Atualiza_Ocorrencia_Produto_gx;
GO

CREATE PROCEDURE dbo.up_04_Atualiza_Ocorrencia_Produto_gx
    @BancoDadosGX VARCHAR(MAX),
    @BancoWF      VARCHAR(MAX)
AS
DECLARE @CMD NVARCHAR(MAX)

IF (NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX))
BEGIN
    PRINT 'O < ' + @BancoDadosGX + ' > INFORMADO NAO EXISTE NESTE SERVIDOR!'
    RETURN
END

PRINT '=========================================================================================='
PRINT ' Atualiza Ocorrencia para VALORES VAZIO/NULO na Tabela Produto_MG '
PRINT '=========================================================================================='

SELECT @CMD = '

    UPDATE a
    SET a.Ocorrencia = CASE
            WHEN VALOR_VENDA IS NULL
                 OR VALOR_VENDA = ''''
                 OR TRY_CAST(RTRIM(LTRIM(VALOR_VENDA)) AS DECIMAL(18, 6)) = 0
            THEN a.Ocorrencia + '' | VALOR_VENDA e INVALIDO.''
            ELSE a.Ocorrencia
        END
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG a
    WHERE a.Flag = 1

    UPDATE a
    SET a.Ocorrencia = CASE
            WHEN VALOR_SUGERIDO IS NULL
                 OR VALOR_SUGERIDO = ''''
                 OR TRY_CAST(RTRIM(LTRIM(VALOR_SUGERIDO)) AS DECIMAL(18, 6)) = 0
            THEN a.Ocorrencia + '' | VALOR_SUGERIDO e INVALIDO.''
            ELSE a.Ocorrencia
        END
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG a
    WHERE a.Flag = 1

    UPDATE a
    SET a.Ocorrencia = CASE
            WHEN VALOR_AQUISICAO IS NULL
                 OR VALOR_AQUISICAO = ''''
                 OR TRY_CAST(RTRIM(LTRIM(VALOR_AQUISICAO)) AS DECIMAL(18, 6)) = 0
            THEN a.Ocorrencia + '' | VALOR_AQUISICAO e INVALIDO.''
            ELSE a.Ocorrencia
        END
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG a
    WHERE a.Flag = 1

    UPDATE a
    SET a.Ocorrencia = CASE
            WHEN VALOR_GARANTIA IS NULL
                 OR VALOR_GARANTIA = ''''
                 OR TRY_CAST(RTRIM(LTRIM(VALOR_GARANTIA)) AS DECIMAL(18, 6)) = 0
            THEN a.Ocorrencia + '' | VALOR_GARANTIA e INVALIDO.''
            ELSE a.Ocorrencia
          END
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG a
    WHERE a.Flag = 1
'
EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' Atualiza Ocorrencia na Tabela Produto_MG UNIDADE_PRODUTO_CODIGO em VAZIO/NULO '
PRINT '=========================================================================================='

SELECT @CMD = '

    UPDATE a
    SET a.Ocorrencia = CASE
            WHEN UNIDADE_PRODUTO_CODIGO IS NULL OR UNIDADE_PRODUTO_CODIGO = ''''
            THEN a.Ocorrencia + '' | UNIDADE_PRODUTO_CODIGO e VAZIO/NULO.''
            ELSE a.Ocorrencia
        END
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG a
    WHERE a.Flag = 1
'
EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' Atualiza Ocorrencia na Tabela Produto_MG TIPO_PRODUTO_CODIGO em VAZIO/NULO '
PRINT '=========================================================================================='

SELECT @CMD = '

    UPDATE a
    SET a.Ocorrencia = CASE
            WHEN TIPO_PRODUTO_CODIGO IS NULL OR TIPO_PRODUTO_CODIGO = ''''
            THEN a.Ocorrencia + '' | TIPO_PRODUTO_CODIGO e VAZIO/NULO.''
            ELSE a.Ocorrencia
        END
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG a
    WHERE a.Flag = 1
'
EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' Atualiza Ocorrencia na Tabela Produto_MG GRUPO_LUCRATIVIDADE_CODIGO em VAZIO/NULO '
PRINT '=========================================================================================='

SELECT @CMD = '

    UPDATE a
    SET a.Ocorrencia = CASE
            WHEN GRUPO_LUCRATIVIDADE_CODIGO IS NULL OR GRUPO_LUCRATIVIDADE_CODIGO = ''''
            THEN a.Ocorrencia + '' | GRUPO_LUCRATIVIDADE_CODIGO e VAZIO/NULO.''
            ELSE a.Ocorrencia
        END
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG a
    WHERE a.Flag = 1
'
EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' Atualiza Ocorrencia na Tabela Produto_MG GRUPO_PRODUTO_CODIGO em VAZIO/NULO '
PRINT '=========================================================================================='

SELECT @CMD = '

    UPDATE a
    SET a.Ocorrencia = CASE
            WHEN GRUPO_PRODUTO_CODIGO IS NULL OR GRUPO_PRODUTO_CODIGO = ''''
            THEN a.Ocorrencia + '' | GRUPO_PRODUTO_CODIGO e VAZIO/NULO.''
            ELSE a.Ocorrencia
        END
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG a
    WHERE a.Flag = 1
'
EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' Atualiza Ocorrencia na Tabela Produto_MG PROCEDENCIA_CODIGO em VAZIO/NULO '
PRINT '=========================================================================================='

SELECT @CMD = '

    UPDATE a
    SET a.Ocorrencia = CASE
            WHEN PROCEDENCIA_CODIGO IS NULL OR PROCEDENCIA_CODIGO = ''''
            THEN a.Ocorrencia + '' | PROCEDENCIA_CODIGO e VAZIO/NULO.''
            ELSE a.Ocorrencia
        END
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG a
    WHERE a.Flag = 1
'
EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' Atualiza PRODUTO_ORIGINAL na Tabela Produto_MG '
PRINT '=========================================================================================='

SELECT @CMD = '

    UPDATE a
    SET a.PRODUTO_ORIGINAL = CASE
            WHEN RTRIM(LTRIM(a.PRODUTO_ORIGINAL)) = ''S'' THEN 1
            WHEN RTRIM(LTRIM(a.PRODUTO_ORIGINAL)) = ''1'' THEN 1
            ELSE 0
        END
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG a
    WHERE a.Flag = 1
'
EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' Atualiza ENVIA_GARANTIA na Tabela Produto_MG '
PRINT '=========================================================================================='

SELECT @CMD = '

    UPDATE a
    SET a.ENVIA_GARANTIA = CASE
            WHEN RTRIM(LTRIM(a.ENVIA_GARANTIA)) = ''S'' THEN 1
            WHEN RTRIM(LTRIM(a.ENVIA_GARANTIA)) = ''1'' THEN 1
            ELSE 0
        END
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG a
    WHERE a.Flag = 1
'
EXEC sp_executesql @CMD

PRINT '=========================================================================================='
PRINT ' AJUSTE de colunas VALOR com "," na Tabela Produto_MG '
PRINT '=========================================================================================='

SELECT @CMD = '

    UPDATE a
    SET a.VALOR_VENDA = REPLACE(a.VALOR_VENDA,'','',''.''),
        a.VALOR_SUGERIDO = REPLACE(a.VALOR_SUGERIDO,'','',''.''),
        a.VALOR_AQUISICAO = REPLACE(a.VALOR_AQUISICAO,'','',''.''),
        a.VALOR_GARANTIA = REPLACE(a.VALOR_GARANTIA,'','',''.'')
    FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG a
    WHERE a.Flag = 1

    ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG ALTER COLUMN VALOR_VENDA FLOAT
    ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG ALTER COLUMN VALOR_SUGERIDO FLOAT
    ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG ALTER COLUMN VALOR_AQUISICAO FLOAT
    ALTER TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.Produto_MG ALTER COLUMN VALOR_GARANTIA FLOAT
'
EXEC sp_executesql @CMD
GO
