/*
========================================================
Database & Object Exploration
========================================================
Script Purpose:
	This script explores the database [MyDatabase], and 
	relevant objects.
========================================================
*/
SELECT * FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_TYPE = 'VIEW';

SELECT * FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'gold' AND TABLE_NAME LIKE ('vw%');
