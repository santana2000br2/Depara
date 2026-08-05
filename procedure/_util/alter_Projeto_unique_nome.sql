-- Unique em NomeProjeto (case-insensitive) para evitar projetos duplicados.
-- Execute no banco principal do DEPARA (onde está a tabela Projeto).
-- Se já existirem nomes duplicados, o CREATE falha — limpe antes com a consulta abaixo.

-- Listar duplicados (se houver):
-- SELECT LOWER(LTRIM(RTRIM(NomeProjeto))) AS NomeNorm, COUNT(*) AS Qtd
-- FROM dbo.Projeto
-- GROUP BY LOWER(LTRIM(RTRIM(NomeProjeto)))
-- HAVING COUNT(*) > 1;

IF EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'UQ_Projeto_NomeProjeto'
      AND object_id = OBJECT_ID('dbo.Projeto')
)
    DROP INDEX UQ_Projeto_NomeProjeto ON dbo.Projeto;
GO

CREATE UNIQUE INDEX UQ_Projeto_NomeProjeto
ON dbo.Projeto (NomeProjeto);
GO
