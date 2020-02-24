
CREATE TABLE #___CHANGED(ID BIGINT, CACHE VARCHAR(255), DB_ACTION VARCHAR(255), SQL_CMD NVARCHAR(MAX), SORT INT DEFAULT(0));
DECLARE @OK BIT = 0;
DECLARE @MESSAGE NVARCHAR(MAX) = '';
------------------------------------
declare @Name nvarchar(50) = ''
declare @Location nvarchar(50) = ''
------------------------------------
--///////////////////////////////////
-- ROLLBACK: START

		INSERT INTO TESTSCHEMA.EMPLOYEES (Name, Location)
		OUTPUT INSERTED.ID, 'POL_EMPLOYEES', 'DB_INSERT', 'SELECT * FROM TESTSCHEMA1.EMPLOYEES WHERE ID = ' + CAST(INSERTED.ID AS VARCHAR(36)) 
		INTO #___CHANGED(ID, CACHE, DB_ACTION, SQL_CMD)
		VALUES (@Name, @Location);


		--///////////////////////////////////
		EXEC ___CHANGED_JSON @OK OUTPUT, @MESSAGE OUTPUT;
		DROP TABLE #___CHANGED;
		-- CHECK TO ROLLBACK, THEN @OK = 0 -> FAIL
		IF @OK = 0  BEGIN  PRINT 'CALL ROLLBACK'; END

-- ROLLBACK: END
--///////////////////////////////////
--SELECT * FROM #___CHANGED;
PRINT @OK;
PRINT @MESSAGE;
