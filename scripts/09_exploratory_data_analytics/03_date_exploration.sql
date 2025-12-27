/*
========================================================================
Date Exploration
========================================================================
Script Purpose:
	This script explores relevant dates across dimension & 
	fact tables, helping to identify the scope of our data.
========================================================================
*/
SELECT
	MIN(birth_date) AS oldest_cust,
	MAX(birth_date) AS youngest_cust,
	DATEDIFF(year, MIN(birth_date), GETDATE()) AS age_oldest_cust,
	DATEDIFF(year, MAX(birth_date), GETDATE()) AS age_youngest_cust,
	DATEDIFF(year, MIN(birth_date), MAX(birth_date)) AS age_range
FROM gold.vw_dim_customers;

SELECT
	MIN(order_date) AS first_order_date,
	MAX(order_date) AS last_order_date,
	DATEDIFF(year, MIN(order_date), MAX(order_date)) AS order_date_scope
FROM gold.vw_fact_sales;
