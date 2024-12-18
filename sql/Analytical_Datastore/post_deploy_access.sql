-- This file contains SQL statements that will be executed after the build script.

DECLARE @group NVARCHAR(100);

-- Groups
-- DAP Alpha - SQL Readers
SET @group = 'DAP Alpha - SQL Readers - ' + UPPER('$(env)');
IF NOT EXISTS (SELECT [name]
FROM [sys].[database_principals]
WHERE [type] = N'X' AND [name] = @group)
    BEGIN
    EXEC ('CREATE USER [' + @group + '] FROM EXTERNAL PROVIDER')
END

-- Roles
EXECUTE sp_addrolemember @rolename = N'db_datareader', @membername = @group;

-- DAP Alpha - SQL Writers
SET @group = 'DAP Alpha - SQL Writers - ' + UPPER('$(env)');
IF NOT EXISTS (SELECT [name]
FROM [sys].[database_principals]
WHERE [type] = N'X' AND [name] = @group)
    BEGIN
    EXEC ('CREATE USER [' + @group + '] FROM EXTERNAL PROVIDER')
END

-- Roles
EXECUTE sp_addrolemember @rolename = N'db_datawriter', @membername = @group;