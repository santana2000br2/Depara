-- =============================================================================
-- Layout: Utilitario - Criticas
-- Staging : (multiplas tabelas _MG)
-- Destino : (consulta)
-- Procedure: up_09_Extrai_Criticas (@BancoDadosGX)
-- =============================================================================

USE [DadosGX_SeuProjeto];  -- ALTERE
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_09_Extrai_Criticas' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_09_Extrai_Criticas;
GO

CREATE PROCEDURE dbo.up_09_Extrai_Criticas
	@BancoDadosGX		VARCHAR(MAX)
	
AS

DECLARE @CMD NVARCHAR(MAX)
-- ==========================================================================

	IF ( NOT EXISTS (SELECT 1 FROM MASTER.DBO.SYSDATABASES WHERE NAME = @BancoDadosGX) )
		BEGIN 
			PRINT 'O < BANCO DE DADOSGX > INFORMADO NAO EXISTE NESTE SERVIDOR!'
			RETURN
		END

-- ==========================================================================================

PRINT '=========================================================================================='
PRINT '	VERIFICANDO CRITICAS DE EXTRAÇÃO DAS TABELAS DE PESSOA'
PRINT '=========================================================================================='

DECLARE @NAME VARCHAR(200)
DECLARE @BANCO VARCHAR(200)

select @BANCO = @BancoDadosGX

DECLARE CUR CURSOR FOR
  SELECT name FROM	sys.objects WHERE name like 'Pessoa_%MG'
  UNION
  SELECT name FROM	sys.objects WHERE name like 'FichaCad_%MG'

OPEN CUR

FETCH NEXT FROM CUR INTO @NAME

WHILE @@FETCH_STATUS = 0
  BEGIN
      SET @CMD = '
		IF ( SELECT COUNT(*) FROM ' + RTRIM(LTRIM(@BANCO)) + '.dbo.' + RTRIM(LTRIM(@NAME)) + ' a WHERE a.Ocorrencia != '''' ) > 0 
		BEGIN
			PRINT '' ENCONTROU CRITICAS DE EXTRAÇÃO DA TABELA <' + RTRIM(LTRIM(@NAME)) + '>''
			
			SELECT a.Ocorrencia,* FROM ' + RTRIM(LTRIM(@BANCO)) + '.dbo.' + RTRIM(LTRIM(@NAME)) + ' a WHERE a.Flag = 0
			
			UNION			
			
			SELECT a.Ocorrencia,* FROM ' + RTRIM(LTRIM(@BANCO)) + '.dbo.' + RTRIM(LTRIM(@NAME)) + ' a WHERE a.Ocorrencia != ''''

			SELECT 
				''' + RTRIM(LTRIM(@NAME)) + ''' AS [TABELA],
				COUNT(*) AS [QTDE DE REGISTROS FLAG = 0 ]				 
			FROM ' + RTRIM(LTRIM(@BANCO)) + '.dbo.' + RTRIM(LTRIM(@NAME)) + ' a
			WHERE 
				a.Flag = 0
			
			SELECT 
				''' + RTRIM(LTRIM(@NAME)) + ''' AS [TABELA],
				COUNT(*) AS [QTDE DE REGISTROS COM OCORRÊNCIAS]				 
			FROM ' + RTRIM(LTRIM(@BANCO)) + '.dbo.' + RTRIM(LTRIM(@NAME)) + ' a 
			WHERE 
				a.Ocorrencia != ''''

		END
		ELSE
			PRINT '' NÃO ENCONTROU CRITICAS DE EXTRAÇÃO DA TABELA <' + RTRIM(LTRIM(@NAME)) + '>''
		'
      --PRINT @CMD
      EXEC Sp_executesql @CMD
	FETCH NEXT FROM CUR INTO @NAME
  END

CLOSE CUR
DEALLOCATE CUR

GO
