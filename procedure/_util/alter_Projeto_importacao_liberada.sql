-- Flag ImportacaoLiberada (somente para tipo Arquivo X Workflow)
-- Executar no banco principal do DE/PARA

IF NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'dbo.Projeto') AND name = N'ImportacaoLiberada'
)
    ALTER TABLE dbo.Projeto ADD ImportacaoLiberada BIT NOT NULL
        CONSTRAINT DF_Projeto_ImportacaoLiberada DEFAULT (0);
GO
