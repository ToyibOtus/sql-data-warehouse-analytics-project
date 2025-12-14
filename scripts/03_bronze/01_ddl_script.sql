/*
=======================================================================
DDL Script: Build Bronze Layer
=======================================================================
Script Purpose:
	This script creates and design the structure of 6 bronze tables.
	Run this script to change the structure of your bronze tables.
=======================================================================
*/
-- Drop Table bronze.crm_cust_info
IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
DROP TABLE bronze.crm_cust_info;
GO

-- Create Table bronze.crm_cust_info
CREATE TABLE bronze.crm_cust_info
(
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_first_name NVARCHAR(50),
	cst_last_name NVARCHAR(50),
	cst_marital_status NVARCHAR(50),
	cst_gndr NVARCHAR(50),
	cst_create_date DATE,
	dwh_step_run_id UNIQUEIDENTIFIER NOT NULL,
	dwh_raw_row NVARCHAR(MAX),
	dwh_row_hash VARBINARY(32),
	dwh_create_date DATETIME DEFAULT GETDATE()
);

-- Drop Table bronze.crm_prd_info
IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
DROP TABLE bronze.crm_prd_info;
GO

-- Create Table bronze.crm_prd_info
CREATE TABLE bronze.crm_prd_info
(
	prd_id INT,
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE,
	dwh_step_run_id UNIQUEIDENTIFIER NOT NULL,
	dwh_raw_row NVARCHAR(MAX),
	dwh_row_hash VARBINARY(32),
	dwh_create_date DATETIME DEFAULT GETDATE()
);

-- Drop Table bronze.crm_sales_details
IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
DROP TABLE bronze.crm_sales_details;
GO

-- Create Table bronze.crm_sales_details
CREATE TABLE bronze.crm_sales_details
(
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT,
	dwh_step_run_id UNIQUEIDENTIFIER NOT NULL,
	dwh_raw_row NVARCHAR(MAX),
	dwh_row_hash VARBINARY(32),
	dwh_create_date DATETIME DEFAULT GETDATE()
);

-- Drop Table bronze.erp_cust_az12
IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
DROP TABLE bronze.erp_cust_az12;
GO

-- Create Table bronze.erp_cust_az12
CREATE TABLE bronze.erp_cust_az12
(
	cid NVARCHAR(50),
	bdate DATE,
	gen NVARCHAR(50),
	dwh_step_run_id UNIQUEIDENTIFIER NOT NULL,
	dwh_raw_row NVARCHAR(MAX),
	dwh_row_hash VARBINARY(32),
	dwh_create_date DATETIME DEFAULT GETDATE()
);

-- Drop Table bronze.erp_loc_a101
IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
DROP TABLE bronze.erp_loc_a101;
GO

-- Create Table bronze.erp_loc_a101
CREATE TABLE bronze.erp_loc_a101
(
	cid NVARCHAR(50),
	cntry NVARCHAR(50),
	dwh_step_run_id UNIQUEIDENTIFIER NOT NULL,
	dwh_raw_row NVARCHAR(MAX),
	dwh_row_hash VARBINARY(32),
	dwh_create_date DATETIME DEFAULT GETDATE()
);

-- Drop Table bronze.erp_px_cat_g1v2
IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
DROP TABLE bronze.erp_px_cat_g1v2;
GO

-- Create Table bronze.erp_px_cat_g1v2
CREATE TABLE bronze.erp_px_cat_g1v2
(
	id NVARCHAR(50),
	cat NVARCHAR(50),
	subcat NVARCHAR(50),
	maintenance NVARCHAR(50),
	dwh_step_run_id UNIQUEIDENTIFIER NOT NULL,
	dwh_raw_row NVARCHAR(MAX),
	dwh_row_hash VARBINARY(32),
	dwh_create_date DATETIME DEFAULT GETDATE()
);
