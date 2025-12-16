/*
==================================================================================================
Create Database & Schemas
==================================================================================================
Script Purpose:
	Creates the database [MyDatabase] and the Medallion Architecture schemas:
    landing, bronze, silver, silver.stg, gold, audit.

Warning:
	This script permanently deletes the database [Mydatabase] and all data in it.
	Ensure to have proper backup before running.
==================================================================================================
*/
USE master;
GO

-- Check existence of [MyDatabase] and Drop it if exist
IF EXISTS(SELECT 1 FROM sys.databases WHERE name = 'MyDatabase')
BEGIN
	ALTER DATABASE MyDatabase SET SINGLE_USER WITH ROLLBACK IMMEDIATE
	DROP DATABASE MyDatabase
END;
GO

-- Create Database [MyDatabase]
CREATE DATABASE MyDatabase;
GO

-- Switch to newly created database
USE MyDatabase;
GO
-- Create schema [landing]
CREATE SCHEMA landing;
GO

-- Create schema [bronze]
CREATE SCHEMA bronze;
GO

-- Create schema [silver]
CREATE SCHEMA silver;
GO

-- Create schema [silver.stg]
CREATE SCHEMA silver.stg
GO

-- Create schema [gold]
CREATE SCHEMA gold;
GO
	
-- Create schema [audit]
CREATE SCHEMA [audit];
GO


