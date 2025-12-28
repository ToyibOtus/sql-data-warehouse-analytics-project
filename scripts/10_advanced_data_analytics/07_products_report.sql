/*
=====================================================================================
Products Report
=====================================================================================
Script Purpose:
	This script generates a well detailed report about our products.It retrieves 
	relevant columns, perform product aggregations & segmentations, and calculates 
	valuable KPIs.
=====================================================================================
*/
IF OBJECT_ID('gold.vw_products_report', 'V') IS NOT NULL
DROP VIEW gold.vw_products_report;
GO

CREATE VIEW gold.vw_products_report AS
-- Retrieve relevant columns
WITH base_query AS
(
SELECT
	dp.product_key,
	dp.product_id,
	dp.product_number,
	dp.product_name,
	dp.product_line,
	dp.category_id,
	dp.category,
	dp.subcategory,
	dp.maintenance,
	dp.product_cost,
	dp.product_start_date,
	fs.order_number,
	fs.customer_key,
	fs.order_date,
	fs.sales,
	fs.quantity,
	fs.price
FROM gold.vw_fact_sales fs
LEFT JOIN gold.vw_dim_products dp
ON fs.product_key = dp.product_key
WHERE order_date IS NOT NULL
)
-- Perform aggregations
, customer_aggregation AS
(
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
	product_start_date,
	MAX(order_date) AS last_order_date,
	DATEDIFF(month, MIN(order_date), MAX(order_date)) AS lifespan_month,
	COUNT(DISTINCT order_number) AS total_orders,
	SUM(quantity) AS total_quantity,
	SUM(sales) AS total_sales,
	COUNT(DISTINCT customer_key) AS total_customers
FROM base_query
GROUP BY
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
)
-- Segment products into various categories
, customer_segmentation AS
(
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
	CASE	
		WHEN lifespan_month >= 12 AND total_sales > 5000 THEN 'High Performer'
		WHEN lifespan_month >= 12 AND total_sales < 5000 THEN 'Mid Performer'
		ELSE 'Low Performer'
	END AS product_status,
	product_start_date,
	last_order_date,
	lifespan_month,
	total_orders,
	total_quantity,
	total_sales,
	total_customers
FROM customer_aggregation
)
-- Calculate Valuable KPIs
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
	product_status,
	product_start_date,
	last_order_date,
	lifespan_month,
	total_orders,
	total_quantity,
	total_sales,
	total_customers,
	DATEDIFF(month, last_order_date, GETDATE()) AS recency_month,
	CASE 
		WHEN total_orders = 0 THEN 0
		ELSE total_sales/total_orders 
	END AS average_order_revenue,
	CASE	
		WHEN lifespan_month = 0 THEN total_sales
		ELSE total_sales/lifespan_month
	END AS average_monthly_revenue
FROM customer_segmentation
