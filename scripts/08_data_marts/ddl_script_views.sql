/*
========================================================================
Views: Build data marts
========================================================================
Script Purpose:
	This script builds data marts, designed and structured to support
	data analytics.

	Run this script to change the structure of your data marts.
========================================================================
*/
-- Create view gold.vw_dim_customers
IF OBJECT_ID('gold.vw_dim_customers', 'V') IS NOT NULL
DROP VIEW gold.vw_dim_customers;
GO

CREATE VIEW gold.vw_dim_customers AS
SELECT
	customer_key,
	customer_id,
	customer_number,
	first_name,
	last_name,
	country,
	gender,
	marital_status,
	birth_date,
	create_date
FROM gold.dim_customers;
GO

-- Create view gold.vw_dim_products
IF OBJECT_ID('gold.vw_dim_products', 'V') IS NOT NULL
DROP VIEW gold.vw_dim_products;
GO

CREATE VIEW gold.vw_dim_products AS
SELECT
	product_key,
	product_id,
	product_number,
	product_name,
	product_line,
	category_id,
	category,
	subcategory,
	maintenance,
	product_cost,
	product_start_date
FROM gold.dim_products;
GO

-- Create view gold.vw_fact_sales
IF OBJECT_ID('gold.vw_fact_sales', 'V') IS NOT NULL
DROP VIEW gold.vw_fact_sales;
GO

CREATE VIEW gold.vw_fact_sales AS
SELECT
	order_number,
	product_key,
	customer_key,
	order_date,
	shipping_date,
	due_date,
	sales,
	quantity,
	price
FROM gold.fact_sales;
