-- =============================================================================
-- Projeto: tipo de migração (3 flags) + datas das fases 1 e 2
-- Executar no banco principal do DE/PARA (tabela dbo.Projeto)
-- Data: 08/07/2026
-- =============================================================================

-- USE [NomeDoBancoDePara];  -- ALTERE conforme ambiente
-- GO

IF NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'dbo.Projeto') AND name = N'TipoWindowsWorkflow'
)
    ALTER TABLE dbo.Projeto ADD TipoWindowsWorkflow BIT NOT NULL
        CONSTRAINT DF_Projeto_TipoWindowsWorkflow DEFAULT (0);
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'dbo.Projeto') AND name = N'TipoWorkflowWorkflow'
)
    ALTER TABLE dbo.Projeto ADD TipoWorkflowWorkflow BIT NOT NULL
        CONSTRAINT DF_Projeto_TipoWorkflowWorkflow DEFAULT (0);
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'dbo.Projeto') AND name = N'TipoArquivoWorkflow'
)
    ALTER TABLE dbo.Projeto ADD TipoArquivoWorkflow BIT NOT NULL
        CONSTRAINT DF_Projeto_TipoArquivoWorkflow DEFAULT (0);
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'dbo.Projeto') AND name = N'Fase1DataInicio'
)
    ALTER TABLE dbo.Projeto ADD Fase1DataInicio DATE NULL;
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'dbo.Projeto') AND name = N'Fase1DataTermino'
)
    ALTER TABLE dbo.Projeto ADD Fase1DataTermino DATE NULL;
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'dbo.Projeto') AND name = N'Fase2DataInicio'
)
    ALTER TABLE dbo.Projeto ADD Fase2DataInicio DATE NULL;
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'dbo.Projeto') AND name = N'Fase2DataTermino'
)
    ALTER TABLE dbo.Projeto ADD Fase2DataTermino DATE NULL;
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'dbo.Projeto') AND name = N'ImportacaoLiberada'
)
    ALTER TABLE dbo.Projeto ADD ImportacaoLiberada BIT NOT NULL
        CONSTRAINT DF_Projeto_ImportacaoLiberada DEFAULT (0);
GO

-- Conferência
SELECT
    c.name AS Coluna,
    t.name AS Tipo,
    c.is_nullable AS PermiteNulo
FROM sys.columns c
INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
WHERE c.object_id = OBJECT_ID(N'dbo.Projeto')
  AND c.name IN (
      'TipoWindowsWorkflow', 'TipoWorkflowWorkflow', 'TipoArquivoWorkflow',
      'Fase1DataInicio', 'Fase1DataTermino', 'Fase2DataInicio', 'Fase2DataTermino',
      'ImportacaoLiberada'
  )
ORDER BY c.column_id;
