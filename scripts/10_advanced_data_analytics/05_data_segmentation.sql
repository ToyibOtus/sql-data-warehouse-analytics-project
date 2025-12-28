/*
==========================================================================
Data Segmentation
==========================================================================
Script Purpose:
	This script segments customers and products into various categories.
==========================================================================
*/
-- What age group do most of our customers fall into?
WITH customers_age AS
(
SELECT
	customer_key,
	first_name,
	last_name,
	DATEDIFF(year, birth_date, GETDATE()) AS age
FROM gold.vw_dim_customers
)
, customer_segmentation AS
(
SELECT
	first_name,
	last_name,
	CASE	
		WHEN age < 20 THEN 'Below 20'
		WHEN age BETWEEN 20 AND 29 THEN '20-29'
		WHEN age BETWEEN 30 AND 39 THEN '30-39'
		WHEN age BETWEEN 40 AND 49 THEN '40-49'
		WHEN age BETWEEN 50 AND 59 THEN '50-59'
		ELSE 'Above 59'
	END AS age_category
FROM customers_age
)
SELECT
	age_category,
	COUNT(*) AS total_customers
FROM customer_segmentation
GROUP BY age_category
ORDER BY total_customers DESC;

-- How many VIPs do we have?
WITH monthly_history AS
(
SELECT
	dc.customer_key,
	dc.first_name,
	dc.last_name,
	MIN(fs.order_date) AS first_order_date,
	MAX(fs.order_date) AS last_order_date,
	DATEDIFF(month, MIN(fs.order_date), MAX(fs.order_date)) AS lifespan_month,
	SUM(fs.sales) AS total_sales
FROM gold.vw_fact_sales fs
LEFT JOIN gold.vw_dim_customers dc
ON fs.customer_key = dc.customer_key
GROUP BY
	dc.customer_key,
	dc.first_name,
	dc.last_name
)
, customer_status AS
(
SELECT
	first_name,
	last_name,
	lifespan_month,
	total_sales,
	CASE	
		WHEN lifespan_month >= 12 AND total_sales > 5000 THEN 'VIP'
		WHEN lifespan_month >= 12 AND total_sales <= 5000 THEN 'Regular'
		ELSE 'New'
	END AS customer_status
FROM monthly_history
)
SELECT
	customer_status,
	COUNT(*) AS total_customers
FROM customer_status
GROUP BY customer_status
ORDER BY total_customers DESC;

-- How many of our products are high performers?
WITH monthly_history AS
(
SELECT
	dp.product_key,
	dp.product_name,
	DATEDIFF(month, MIN(fs.order_date), MAX(fs.order_date)) AS lifespan_month,
	SUM(fs.sales) AS total_sales
FROM gold.vw_fact_sales fs
LEFT JOIN gold.vw_dim_products dp
ON fs.product_key = dp.product_key
GROUP BY
	dp.product_key,
	dp.product_name
)
, product_status AS
(
SELECT
	product_key,
	product_name,
	lifespan_month,
	total_sales,
	CASE	
		WHEN lifespan_month >= 12 AND total_sales > 5000 THEN 'High Performer'
		WHEN lifespan_month >= 12 AND total_sales <= 5000 THEN 'Mid Performer'
		ELSE 'Low Performer'
	END AS product_status
FROM monthly_history
)
SELECT
	product_status,
	COUNT(*) AS total_products
FROM product_status
GROUP BY product_status
ORDER BY total_products DESC;
