/*
=======================================================================
DDL Script: Build Landing Layer
=======================================================================
Script Purpose:
	This script creates and design the structure of 6 landing tables.
	Run this script to change the structure of your landing tables.
=======================================================================
*/
-- Drop Table landing.crm_cust_info
IF OBJECT_ID('landing.crm_cust_info', 'U') IS NOT NULL
DROP TABLE landing.crm_cust_info;
GO

-- Create Table landing.crm_cust_info
CREATE TABLE landing.crm_cust_info
(
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_first_name NVARCHAR(50),
	cst_last_name NVARCHAR(50),
	cst_marital_status NVARCHAR(50),
	cst_gndr NVARCHAR(50),
	cst_create_date DATE
);

-- Drop Table landing.crm_prd_info
IF OBJECT_ID('landing.crm_prd_info', 'U') IS NOT NULL
DROP TABLE landing.crm_prd_info;
GO

-- Create Table landing.crm_prd_info
CREATE TABLE landing.crm_prd_info
(
	prd_id INT,
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE
);

-- Drop Table landing.crm_sales_details
IF OBJECT_ID('landing.crm_sales_details', 'U') IS NOT NULL
DROP TABLE landing.crm_sales_details;
GO

-- Create Table landing.crm_sales_details
CREATE TABLE landing.crm_sales_details
(
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT
);

-- Drop Table landing.erp_cust_az12
IF OBJECT_ID('landing.erp_cust_az12', 'U') IS NOT NULL
DROP TABLE landing.erp_cust_az12;
GO

-- Create Table landing.erp_cust_az12
CREATE TABLE landing.erp_cust_az12
(
	cid NVARCHAR(50),
	bdate DATE,
	gen NVARCHAR(50)
);

-- Drop Table landing.erp_loc_a101
IF OBJECT_ID('landing.erp_loc_a101', 'U') IS NOT NULL
DROP TABLE landing.erp_loc_a101;
GO

-- Create Table landing.erp_loc_a101
CREATE TABLE landing.erp_loc_a101
(
	cid NVARCHAR(50),
	cntry NVARCHAR(50)
);

-- Drop Table landing.erp_px_cat_g1v2
IF OBJECT_ID('landing.erp_px_cat_g1v2', 'U') IS NOT NULL
DROP TABLE landing.erp_px_cat_g1v2;
GO

-- Create Table landing.erp_px_cat_g1v2
CREATE TABLE landing.erp_px_cat_g1v2
(
	id NVARCHAR(50),
	cat NVARCHAR(50),
	subcat NVARCHAR(50),
	maintenance NVARCHAR(50)
);
