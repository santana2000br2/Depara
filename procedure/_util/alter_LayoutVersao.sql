-- Versionamento de layouts: guarda o histórico de cada alteração de um layout
-- e permite restaurar uma versão anterior.
-- Executar no banco principal do DE/PARA.

-- Cabeçalho da versão (um snapshot do layout em determinado momento)
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'LayoutVersoes' AND schema_id = SCHEMA_ID(N'dbo'))
BEGIN
    CREATE TABLE dbo.LayoutVersoes (
        LayoutVersaoID INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        LayoutID INT NOT NULL,
        Versao INT NOT NULL,
        NomeLayout NVARCHAR(100) NOT NULL,
        Descricao NVARCHAR(500) NULL,
        TipoAcao NVARCHAR(20) NOT NULL
            CONSTRAINT DF_LayoutVersoes_TipoAcao DEFAULT (N'edicao'),
        Observacao NVARCHAR(255) NULL,
        UsuarioVersao INT NULL,
        DataVersao DATETIME NOT NULL
            CONSTRAINT DF_LayoutVersoes_Data DEFAULT (GETDATE()),
        CONSTRAINT UQ_LayoutVersoes_Layout_Versao UNIQUE (LayoutID, Versao)
    );
END
GO

-- Colunas de cada versão (snapshot de LayoutColunas)
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'LayoutVersaoColunas' AND schema_id = SCHEMA_ID(N'dbo'))
BEGIN
    CREATE TABLE dbo.LayoutVersaoColunas (
        LayoutVersaoColunaID INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        LayoutVersaoID INT NOT NULL,
        Posicao INT NOT NULL,
        Descricao NVARCHAR(100) NOT NULL,
        Obrigatorio BIT NOT NULL
            CONSTRAINT DF_LayoutVersaoColunas_Obrigatorio DEFAULT (0),
        Validacao NVARCHAR(100) NULL,
        TipoDado NVARCHAR(50) NOT NULL
            CONSTRAINT DF_LayoutVersaoColunas_TipoDado DEFAULT (N'texto'),
        CONSTRAINT FK_LayoutVersaoColunas_Versao
            FOREIGN KEY (LayoutVersaoID) REFERENCES dbo.LayoutVersoes (LayoutVersaoID)
            ON DELETE CASCADE
    );

    CREATE INDEX IX_LayoutVersaoColunas_Versao
        ON dbo.LayoutVersaoColunas (LayoutVersaoID);
END
GO
