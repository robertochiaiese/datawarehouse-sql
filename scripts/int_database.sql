/*
==========================================================
CREATE DATABASE AND SCHEMAS
==========================================================
Scritp Purpose:
  The script creates the database called 'DataWarehouse', in case the database already exists it will be dropped
  in order to be replaced by a new one.
  Afterwards, the three schemas are generated: bronze, silver and gold.
*/


USE master;

IF EXISTS ( SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO


--Create & Use the Database 
CREATE DATABASE  DataWarehouse;
GO

USE DataWarehouse;
GO


--Create Three Schemas( Bronze, Silver, Gold)
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
