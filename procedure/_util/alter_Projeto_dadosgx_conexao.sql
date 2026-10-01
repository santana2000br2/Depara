-- Credenciais de conexão do DadosGX (servidor distinto da homologação WF)
-- Executar no banco principal do DE/PARA

IF NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'dbo.Projeto') AND name = N'servidordadosgx'
)
    ALTER TABLE dbo.Projeto ADD servidordadosgx NVARCHAR(255) NULL;
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'dbo.Projeto') AND name = N'usuariodadosgx'
)
    ALTER TABLE dbo.Projeto ADD usuariodadosgx NVARCHAR(500) NULL;
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'dbo.Projeto') AND name = N'senhadadosgx'
)
    ALTER TABLE dbo.Projeto ADD senhadadosgx NVARCHAR(500) NULL;
GO
