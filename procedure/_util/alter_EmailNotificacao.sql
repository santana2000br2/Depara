-- Configuração global de envio automático de e-mail para qualquer bloco De/Para
-- Executar no banco principal do DE/PARA

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'EmailSmtpConfig' AND schema_id = SCHEMA_ID(N'dbo'))
BEGIN
    CREATE TABLE dbo.EmailSmtpConfig (
        EmailSmtpConfigID INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        ProjetoID INT NOT NULL,
        SmtpHost NVARCHAR(255) NOT NULL,
        SmtpPort INT NOT NULL CONSTRAINT DF_EmailSmtpConfig_Port DEFAULT (587),
        SmtpUsuario NVARCHAR(255) NULL,
        SmtpSenha NVARCHAR(500) NULL,
        EmailRemetente NVARCHAR(255) NOT NULL,
        Destinatarios NVARCHAR(MAX) NOT NULL
            CONSTRAINT DF_EmailSmtpConfig_Destinatarios DEFAULT (''),
        UsarTls BIT NOT NULL CONSTRAINT DF_EmailSmtpConfig_Tls DEFAULT (1),
        Ativo BIT NOT NULL CONSTRAINT DF_EmailSmtpConfig_Ativo DEFAULT (1),
        CONSTRAINT UQ_EmailSmtpConfig_Projeto UNIQUE (ProjetoID)
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'dbo.EmailSmtpConfig')
      AND name = N'Destinatarios'
)
    ALTER TABLE dbo.EmailSmtpConfig ADD Destinatarios NVARCHAR(MAX) NOT NULL
        CONSTRAINT DF_EmailSmtpConfig_Destinatarios DEFAULT ('');
GO

-- Migra listas cadastradas na versão anterior (destinatários por bloco).
IF OBJECT_ID(N'dbo.EmailNotificacaoBloco', N'U') IS NOT NULL
BEGIN
    UPDATE smtp
       SET Destinatarios = legado.Destinatarios
      FROM dbo.EmailSmtpConfig smtp
      CROSS APPLY (
          SELECT STUFF((
              SELECT N', ' + bloco.Destinatarios
                FROM dbo.EmailNotificacaoBloco bloco
               WHERE bloco.ProjetoID = smtp.ProjetoID
                 AND NULLIF(LTRIM(RTRIM(bloco.Destinatarios)), N'') IS NOT NULL
               FOR XML PATH(''), TYPE
          ).value('.', 'NVARCHAR(MAX)'), 1, 2, N'') AS Destinatarios
      ) legado
     WHERE NULLIF(LTRIM(RTRIM(smtp.Destinatarios)), N'') IS NULL
       AND NULLIF(LTRIM(RTRIM(legado.Destinatarios)), N'') IS NOT NULL;
END
GO

-- Converte uma configuração antiga por projeto em configuração global (ProjetoID = 0).
IF NOT EXISTS (SELECT 1 FROM dbo.EmailSmtpConfig WHERE ProjetoID = 0)
   AND EXISTS (SELECT 1 FROM dbo.EmailSmtpConfig)
BEGIN
    INSERT INTO dbo.EmailSmtpConfig (
        ProjetoID, SmtpHost, SmtpPort, SmtpUsuario, SmtpSenha,
        EmailRemetente, Destinatarios, UsarTls, Ativo
    )
    SELECT TOP (1)
        0, SmtpHost, SmtpPort, SmtpUsuario, SmtpSenha,
        EmailRemetente, Destinatarios, UsarTls, Ativo
    FROM dbo.EmailSmtpConfig
    ORDER BY EmailSmtpConfigID DESC;
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'EmailNotificacaoEnviada' AND schema_id = SCHEMA_ID(N'dbo'))
BEGIN
    CREATE TABLE dbo.EmailNotificacaoEnviada (
        EmailNotificacaoEnviadaID INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        ProjetoID INT NOT NULL,
        BlocoEscopo NVARCHAR(50) NOT NULL,
        DataEnvio DATETIME NOT NULL CONSTRAINT DF_EmailNotificacaoEnviada_Data DEFAULT (GETDATE()),
        Destinatarios NVARCHAR(MAX) NULL,
        CONSTRAINT UQ_EmailNotificacaoEnviada UNIQUE (ProjetoID, BlocoEscopo)
    );
END
GO
