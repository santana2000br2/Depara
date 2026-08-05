-- Data em que cada bloco De/Para ficou disponível após importação do layout.
-- Executar no banco principal do DE/PARA.

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'BlocoDeParaDisponivel' AND schema_id = SCHEMA_ID(N'dbo'))
BEGIN
    CREATE TABLE dbo.BlocoDeParaDisponivel (
        BlocoDeParaDisponivelID INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        ProjetoID INT NOT NULL,
        BlocoEscopo NVARCHAR(50) NOT NULL,
        DataDisponivel DATETIME NOT NULL
            CONSTRAINT DF_BlocoDeParaDisponivel_Data DEFAULT (GETDATE()),
        DataUltimaImportacao DATETIME NOT NULL
            CONSTRAINT DF_BlocoDeParaDisponivel_Ultima DEFAULT (GETDATE()),
        TipoLayout NVARCHAR(80) NULL,
        NomeLayout NVARCHAR(100) NULL,
        UsuarioID INT NULL,
        CONSTRAINT UQ_BlocoDeParaDisponivel UNIQUE (ProjetoID, BlocoEscopo)
    );

    CREATE INDEX IX_BlocoDeParaDisponivel_Projeto
        ON dbo.BlocoDeParaDisponivel (ProjetoID);
END
GO
