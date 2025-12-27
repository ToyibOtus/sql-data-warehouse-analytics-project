/*
========================================================================
Dimension Exploration
========================================================================
Script Purpose:
	This script explores dimensions across dimension tables.
========================================================================
*/
SELECT DISTINCT country FROM gold.vw_dim_customers;

SELECT DISTINCT gender FROM gold.vw_dim_customers;

SELECT DISTINCT marital_status FROM gold.vw_dim_customers;

SELECT DISTINCT product_line, category, subcategory, product_name FROM gold.vw_dim_products;

SELECT DISTINCT maintenance FROM gold.vw_dim_products;
