/*
=====================================================================================
Customers Report
=====================================================================================
Script Purpose:
	This script generates a well detailed report about our customers.It retrieves 
	relevant columns, perform customer aggregations & segmentations, and calculates 
	valuable KPIs.
=====================================================================================
*/
IF OBJECT_ID('gold.vw_customers_report', 'V') IS NOT NULL
DROP VIEW gold.vw_customers_report;
GO

CREATE VIEW gold.vw_customers_report AS
-- Retrieve relevant columns
WITH base_query AS
(
SELECT
	dc.customer_key,
	dc.customer_id,
	dc.customer_number,
	dc.first_name,
	dc.last_name,
	dc.country,
	dc.gender,
	dc.marital_status,
	dc.birth_date,
	dc.create_date,
	fs.order_number,
	fs.product_key,
	fs.order_date,
	fs.sales,
	fs.quantity,
	fs.price
FROM gold.vw_fact_sales fs
LEFT JOIN gold.vw_dim_customers dc
ON fs.customer_key = dc.customer_key
WHERE order_date IS NOT NULL
)
-- Perform aggregations
, customer_aggregation AS
(
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
	DATEDIFF(year, birth_date, GETDATE()) AS age,
	create_date,
	MIN(order_date) AS first_order_date,
	MAX(order_date) AS last_order_date,
	DATEDIFF(month, MIN(order_date), MAX(order_date)) AS lifespan_month,
	COUNT(DISTINCT order_number) AS total_orders,
	SUM(quantity) AS total_quantity,
	SUM(sales) AS total_sales,
	COUNT(DISTINCT product_key) AS total_products
FROM base_query
GROUP BY
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
)
-- Segment customers into various categories
, customer_segmentation AS
(
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
	age,
	CASE
		WHEN age < 20 THEN 'Below 20'
		WHEN age BETWEEN 20 AND 29 THEN '20-29'
		WHEN age BETWEEN 30 AND 39 THEN '30-39'
		WHEN age BETWEEN 40 AND 49 THEN '40-49'
		WHEN age BETWEEN 50 AND 59 THEN '50-59'
		ELSE 'Above 60'
	END AS age_group,
	CASE	
		WHEN lifespan_month >= 12 AND total_sales > 5000 THEN 'VIP'
		WHEN lifespan_month >= 12 AND total_sales < 5000 THEN 'Regular'
		ELSE 'New'
	END AS customer_status,
	create_date,
	first_order_date,
	last_order_date,
	lifespan_month,
	total_orders,
	total_quantity,
	total_sales,
	total_products
FROM customer_aggregation
)
-- Calculate Valuable KPIs
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
	age,
	age_group,
	customer_status,
	create_date,
	first_order_date,
	last_order_date,
	lifespan_month,
	total_orders,
	total_quantity,
	total_sales,
	total_products,
	DATEDIFF(month, last_order_date, GETDATE()) AS recency_month,
	CASE 
		WHEN total_orders = 0 THEN 0
		ELSE total_sales/total_orders 
	END AS average_order_value,
	CASE	
		WHEN lifespan_month = 0 THEN total_sales
		ELSE total_sales/lifespan_month
	END AS average_monthly_spend
FROM customer_segmentation
