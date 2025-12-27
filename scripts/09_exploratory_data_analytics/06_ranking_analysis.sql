/*
============================================================================
Ranking Analysis
============================================================================
Script Purpose:
	This script ranks dimension based on relevant business metrics.
============================================================================
*/
-- Who are our top 3 customers based on total number of orders?
SELECT
	customer_key,
	first_name,
	last_name,
	total_orders,
	rank_customers_orders
FROM
(
SELECT
	dc.customer_key,
	dc.first_name,
	dc.last_name,
	COUNT(DISTINCT fs.order_number) AS total_orders,
	DENSE_RANK() OVER(ORDER BY COUNT(DISTINCT fs.order_number) DESC) AS rank_customers_orders
FROM gold.vw_fact_sales fs
LEFT JOIN gold.vw_dim_customers dc
ON fs.customer_key = dc.customer_key
GROUP BY
	dc.customer_key,
	dc.first_name,
	dc.last_name
)SUB
WHERE rank_customers_orders <= 3;

-- How does the rank compare to total quantity of products purchased?
SELECT
	customer_key,
	first_name,
	last_name,
	total_quantity,
	rank_customers_quantity
FROM
(
SELECT
	dc.customer_key,
	dc.first_name,
	dc.last_name,
	SUM(fs.quantity) AS total_quantity,
	DENSE_RANK() OVER(ORDER BY SUM(fs.quantity) DESC) AS rank_customers_quantity
FROM gold.vw_fact_sales fs
LEFT JOIN gold.vw_dim_customers dc
ON fs.customer_key = dc.customer_key
GROUP BY
	dc.customer_key,
	dc.first_name,
	dc.last_name
)SUB
WHERE rank_customers_quantity <= 5;

-- Who are the top 5 customers that bring in the most revenue?
SELECT
	customer_key,
	first_name,
	last_name,
	total_sales,
	rank_customers_sales
FROM
(
SELECT
	dc.customer_key,
	dc.first_name,
	dc.last_name,
	SUM(fs.sales) AS total_sales,
	DENSE_RANK() OVER(ORDER BY SUM(fs.sales) DESC) AS rank_customers_sales
FROM gold.vw_fact_sales fs
LEFT JOIN gold.vw_dim_customers dc
ON fs.customer_key = dc.customer_key
GROUP BY
	dc.customer_key,
	dc.first_name,
	dc.last_name
)SUB
WHERE rank_customers_sales <= 5;

-- What are the top 5 products based on total orders
SELECT
	product_name,
	total_orders,
	rank_product_orders
FROM
(
SELECT
	dp.product_key,
	dp.product_name,
	COUNT(DISTINCT fs.order_number) AS total_orders,
	DENSE_RANK() OVER(ORDER BY COUNT(DISTINCT fs.order_number) DESC) AS rank_product_orders
FROM gold.vw_fact_sales fs
LEFT JOIN gold.vw_dim_products dp
ON fs.product_key = dp.product_key
GROUP BY dp.product_key, dp.product_name
)SUB
WHERE rank_product_orders <= 5;

-- How does the rank vary in comparison to total quantity purchased
SELECT
	product_name,
	total_quantity,
	rank_product_quantity
FROM
(
SELECT
	dp.product_key,
	dp.product_name,
	SUM(fs.quantity) AS total_quantity,
	DENSE_RANK() OVER(ORDER BY SUM(fs.quantity) DESC) AS rank_product_quantity
FROM gold.vw_fact_sales fs
LEFT JOIN gold.vw_dim_products dp
ON fs.product_key = dp.product_key
GROUP BY dp.product_key, dp.product_name
)SUB
WHERE rank_product_quantity <= 5;

-- What are the top 5 products that bring in the most revenue?
SELECT
	product_name,
	total_sales,
	rank_product_sales
FROM
(
SELECT
	dp.product_key,
	dp.product_name,
	SUM(fs.sales) AS total_sales,
	DENSE_RANK() OVER(ORDER BY SUM(fs.sales) DESC) AS rank_product_sales
FROM gold.vw_fact_sales fs
LEFT JOIN gold.vw_dim_products dp
ON fs.product_key = dp.product_key
GROUP BY dp.product_key, dp.product_name
)SUB
WHERE rank_product_sales <= 5;
