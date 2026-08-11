-- =============================================================================
-- Layout: Utilitario
-- Staging : (n/a)
-- Destino : (n/a)
-- Procedure: up_Replace_Name_DadosGx_Procedures (@BancoDadosGX)
-- Autor: Aroldo Santana
-- Data de alteração: 06/07/2026
-- =============================================================================

USE [DadosGX_SeuProjeto];  -- ALTERE para o nome real do banco DadosGX
GO

IF EXISTS (SELECT 1 FROM sys.procedures WHERE NAME = 'up_Replace_Name_DadosGx_Procedures' AND TYPE = 'P')
    DROP PROCEDURE dbo.up_Replace_Name_DadosGx_Procedures;
GO

CREATE PROCEDURE [dbo].up_Replace_Name_DadosGx_Procedures
	@BancoDadosGX VARCHAR(MAX)

AS

DECLARE @CMD NVARCHAR(MAX)

PRINT '==========================================================================================' 
PRINT ' PREPARAÇÃO 01 - (up_Replace_Name_DadosGx_Procedures)' 
PRINT '=========================================================================================='

SELECT @cmd = '

	IF EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects WHERE NAME = ''tb_temp_proc1'' AND TYPE = ''U'')
		BEGIN 
			DROP TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.tb_temp_proc1
		END 

	IF EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects WHERE NAME = ''tb_temp_proc2'' AND TYPE = ''U'')
		BEGIN 
			DROP TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.tb_temp_proc2
		END 

	IF EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects WHERE NAME = ''tb_temp_proc3'' AND TYPE = ''U'')
		BEGIN 
			DROP TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.tb_temp_proc3
		END 

'

EXEC sp_executesql @cmd

PRINT '==========================================================================================' 
PRINT ' PREPARAÇÃO 02 - (up_Replace_Name_DadosGx_Procedures)' 
PRINT '=========================================================================================='

SELECT @cmd = '

	CREATE TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.tb_temp_proc1 (object_id int, definition varchar(max));

	CREATE TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.tb_temp_proc2 (object_id int, definition varchar(max));

	CREATE TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.tb_temp_proc3 (object_id int, definition varchar(max));

'

EXEC sp_executesql @cmd

PRINT '==========================================================================================' 
PRINT ' PREPARAÇÃO 03 - (up_Replace_Name_DadosGx_Procedures)' 
PRINT '=========================================================================================='

SELECT @cmd = '

	INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.tb_temp_proc1
	SELECT
		object_id, definition
	FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.Sys.sql_modules
	WHERE 
		definition LIKE ''%DadosGx_ViaVR_AbrMai%'';                  -- TEXTO QUE CONSTA NA PROCEDURE
'

EXEC sp_executesql @cmd

PRINT '==========================================================================================' 
PRINT ' PREPARAÇÃO 04 - (up_Replace_Name_DadosGx_Procedures)' 
PRINT '=========================================================================================='

SELECT @cmd = '

	INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.tb_temp_proc2
	SELECT
		object_id,
		replace(definition,''DadosGx_ViaVR_AbrMai'',''' + LTRIM(RTRIM(@BancoDadosGX)) + ''')           -- REPLACE NO TEXTO QUE CONSTA NA PROCEDURE
	FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.tb_temp_proc1;
'

EXEC sp_executesql @cmd

PRINT '==========================================================================================' 
PRINT ' PREPARAÇÃO 05 - (up_Replace_Name_DadosGx_Procedures)' 
PRINT '=========================================================================================='

SELECT @cmd = '

	INSERT INTO ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.tb_temp_proc3
	SELECT
		object_id,
		replace(definition,''CREATE PROCEDURE'',''ALTER PROCEDURE'')		-- ALTER PROCEDURE
	FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.tb_temp_proc2;
'

EXEC sp_executesql @cmd

PRINT '==========================================================================================' 
PRINT ' PREPARAÇÃO 06 - (up_Replace_Name_DadosGx_Procedures)' 
PRINT '=========================================================================================='

SELECT @CMD = '

	DECLARE @DEFINITION AS varchar(max);
	DECLARE @OBJECT_ID AS int;
	DECLARE CursorProc1 CURSOR FOR

		SELECT object_id, definition FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.tb_temp_proc3;

	OPEN CursorProc1;

		FETCH NEXT FROM CursorProc1  INTO @OBJECT_ID, @DEFINITION;

		WHILE @@FETCH_STATUS = 0
		BEGIN

			PRINT ''Compilando: ''+object_name(@OBJECT_ID)

			EXEC (@DEFINITION);

			FETCH NEXT FROM CursorProc1 INTO @OBJECT_ID, @DEFINITION;

		END;

	CLOSE CursorProc1;

	DEALLOCATE CursorProc1;

'

EXEC sp_executesql @CMD

PRINT '==========================================================================================' 
PRINT ' PREPARAÇÃO 07 -  (up_Replace_Name_DadosGx_Procedures)' 
PRINT '=========================================================================================='

SELECT @CMD = '

	IF EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects WHERE NAME = ''tb_temp_proc1'' AND TYPE = ''U'')
		BEGIN 
			DROP TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.tb_temp_proc1
		end 

	IF EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects WHERE NAME = ''tb_temp_proc2'' AND TYPE = ''U'')
		BEGIN 
			DROP TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.tb_temp_proc2
		end 

	IF EXISTS (SELECT 1 FROM ' + LTRIM(RTRIM(@BancoDadosGX)) + '.sys.objects WHERE NAME = ''tb_temp_proc3'' AND TYPE = ''U'')
		BEGIN 
			DROP TABLE ' + LTRIM(RTRIM(@BancoDadosGX)) + '.dbo.tb_temp_proc3
		end 

'

EXEC sp_executesql @CMD

--	BEGIN
--	 PRINT 'QUERY EXECUTADA COM SUCESSO'
--	END
	
--GO 

--/* EXEC */
--EXEC dbo.up_Replace_Name_DadosGx_Procedures @BancoDadosGX

GO
