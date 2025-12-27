/*
============================================================================
Magnitude Analysis
============================================================================
Script Purpose:
	This script reveals how key business metrics such as sales, quantity
	etc. are distributed across various dimensions, and thus
	highlighting their magnitude.
============================================================================
*/
-- Which country do most of our customers reside in?
SELECT
	country,
	COUNT(*) AS total_customers
FROM gold.vw_dim_customers
GROUP BY country
ORDER BY total_customers DESC;

-- Are most of our customers males?
SELECT
	gender,
	COUNT(*) AS total_customers
FROM gold.vw_dim_customers
WHERE gender != 'Unknown'
GROUP BY gender
ORDER BY total_customers DESC;

-- Are most of our customers married?
SELECT
	marital_status,
	COUNT(*) AS total_customers
FROM gold.vw_dim_customers
GROUP BY marital_status
ORDER BY total_customers DESC;

-- Which country drives in the most revenue?
SELECT
	dc.country,
	COUNT(DISTINCT dc.customer_key) AS total_cust,
	COUNT(DISTINCT fs.customer_key) AS total_cust_ordered,
	(CAST(COUNT(DISTINCT fs.customer_key) AS FLOAT)/COUNT(DISTINCT dc.customer_key)) * 100 AS percent_cust_ordered,
	SUM(COALESCE(fs.price * fs.quantity, 0))/SUM(NULLIF(fs.quantity, 0)) AS weighted_avg_price,
	COUNT(DISTINCT fs.product_key) AS total_products,
	COUNT(DISTINCT fs.order_number) AS total_orders,
	SUM(COALESCE(fs.quantity, 0)) AS total_quantity,
	SUM(COALESCE(fs.sales, 0)) AS total_sales
FROM gold.vw_dim_customers dc
LEFT JOIN gold.vw_fact_sales fs
ON dc.customer_key = fs.customer_key
WHERE country != 'Unknown'
GROUP BY dc.country
ORDER BY total_sales DESC;

-- Do females spend more than males?
SELECT
	dc.gender,
	COUNT(DISTINCT dc.customer_key) AS total_cust,
	COUNT(DISTINCT fs.customer_key) AS total_cust_ordered,
	(CAST(COUNT(DISTINCT fs.customer_key) AS FLOAT)/COUNT(DISTINCT dc.customer_key)) * 100 AS percent_cust_ordered,
	SUM(COALESCE(fs.price * fs.quantity, 0))/SUM(NULLIF(fs.quantity, 0)) AS weighted_avg_price,
	COUNT(DISTINCT fs.product_key) AS total_products,
	COUNT(DISTINCT fs.order_number) AS total_orders,
	SUM(COALESCE(fs.quantity, 0)) AS total_quantity,
	SUM(COALESCE(fs.sales, 0)) AS total_sales
FROM gold.vw_dim_customers dc
LEFT JOIN gold.vw_fact_sales fs
ON dc.customer_key = fs.customer_key
WHERE dc.gender != 'Unknown'
GROUP BY dc.gender
ORDER BY total_sales DESC;

-- Do married customers generate more revenue than their single counterparts?
SELECT
	dc.marital_status,
	COUNT(DISTINCT dc.customer_key) AS total_cust,
	COUNT(DISTINCT fs.customer_key) AS total_cust_ordered,
	(CAST(COUNT(DISTINCT fs.customer_key) AS FLOAT)/COUNT(DISTINCT dc.customer_key)) * 100 AS percent_cust_ordered,
	SUM(COALESCE(fs.price * fs.quantity, 0))/SUM(NULLIF(fs.quantity, 0)) AS weighted_avg_price,
	COUNT(DISTINCT fs.product_key) AS total_products,
	COUNT(DISTINCT fs.order_number) AS total_orders,
	SUM(COALESCE(fs.quantity, 0)) AS total_quantity,
	SUM(COALESCE(fs.sales, 0)) AS total_sales
FROM gold.vw_dim_customers dc
LEFT JOIN gold.vw_fact_sales fs
ON dc.customer_key = fs.customer_key
WHERE dc.marital_status != 'Unknown'
GROUP BY dc.marital_status
ORDER BY total_sales DESC;

-- What product line does the organization seem to prioritize?
SELECT
	product_line,
	COUNT(*) AS total_products
FROM gold.vw_dim_products
WHERE product_line != 'Unknown'
GROUP BY product_line
ORDER BY total_products DESC;

-- What is the total number of each category across all product line?
SELECT
	product_line,
	category,
	COUNT(*) AS total_products
FROM gold.vw_dim_products
WHERE product_line != 'Unknown' AND category IS NOT NULL
GROUP BY product_line, category
ORDER BY product_line, total_products DESC;

-- How does the count vary when subcategory is thrown into the picture?
SELECT
	product_line,
	category,
	subcategory,
	COUNT(*) AS total_products
FROM gold.vw_dim_products
WHERE product_line != 'Unknown' AND category IS NOT NULL
GROUP BY product_line, category, subcategory
ORDER BY product_line, total_products DESC;

-- Which product line generates the most revenue? and why?
SELECT
	dp.product_line,
	COUNT(DISTINCT dp.product_key) AS total_products,
	COUNT(DISTINCT fs.product_key) AS total_products_ordered,
	ROUND(CAST(COUNT(DISTINCT fs.product_key) AS FLOAT)/COUNT(DISTINCT dp.product_key) * 100, 2) AS percent_products_ordered ,
	SUM(COALESCE(fs.price * fs.quantity, 0))/SUM(NULLIF(quantity, 0)) AS weighted_avg_price,
	COUNT(DISTINCT fs.customer_key) AS total_customers,
	COUNT(DISTINCT fs.order_number) AS total_orders,
	SUM(COALESCE(fs.quantity, 0)) AS total_quantity,
	SUM(COALESCE(fs.sales, 0)) AS total_sales
FROM gold.vw_dim_products dp
LEFT JOIN gold.vw_fact_sales fs
ON dp.product_key = fs.product_key
WHERE product_line != 'Unknown'
GROUP BY dp.product_line
ORDER BY total_sales DESC;

-- How is this revenue distributed across categories?
SELECT
	dp.product_line,
	dp.category,
	COUNT(DISTINCT dp.product_key) AS total_products,
	COUNT(DISTINCT fs.product_key) AS total_products_ordered,
	ROUND(CAST(COUNT(DISTINCT fs.product_key) AS FLOAT)/COUNT(DISTINCT dp.product_key) * 100, 2) AS percent_products_ordered ,
	SUM(COALESCE(fs.price * fs.quantity, 0))/SUM(NULLIF(quantity, 0)) AS weighted_avg_price,
	COUNT(DISTINCT fs.customer_key) AS total_customers,
	COUNT(DISTINCT fs.order_number) AS total_orders,
	SUM(COALESCE(fs.quantity, 0)) AS total_quantity,
	SUM(COALESCE(fs.sales, 0)) AS total_sales
FROM gold.vw_dim_products dp
LEFT JOIN gold.vw_fact_sales fs
ON dp.product_key = fs.product_key
WHERE product_line != 'Unknown' AND category IS NOT NULL
GROUP BY dp.product_line, dp.category
ORDER BY dp.product_line, total_sales DESC;

-- How is it distributed when subcategory is thrown into the picture?
SELECT
	dp.product_line,
	dp.category,
	dp.subcategory,
	COUNT(DISTINCT dp.product_key) AS total_products,
	COUNT(DISTINCT fs.product_key) AS total_products_ordered,
	ROUND(CAST(COUNT(DISTINCT fs.product_key) AS FLOAT)/COUNT(DISTINCT dp.product_key) * 100, 2) AS percent_products_ordered ,
	SUM(COALESCE(fs.price * fs.quantity, 0))/SUM(NULLIF(quantity, 0)) AS weighted_avg_price,
	COUNT(DISTINCT fs.customer_key) AS total_customers,
	COUNT(DISTINCT fs.order_number) AS total_orders,
	SUM(COALESCE(fs.quantity, 0)) AS total_quantity,
	SUM(COALESCE(fs.sales, 0)) AS total_sales
FROM gold.vw_dim_products dp
LEFT JOIN gold.vw_fact_sales fs
ON dp.product_key = fs.product_key
WHERE product_line != 'Unknown' AND category IS NOT NULL
GROUP BY dp.product_line, dp.category, dp.subcategory
ORDER BY dp.product_line, dp.category, total_sales DESC;

-- How is revenue distributed across all products?
SELECT
	dp.product_line,
	dp.category,
	dp.subcategory,
	dp.product_name,
	COUNT(DISTINCT dp.product_key) AS total_products,
	COUNT(DISTINCT fs.product_key) AS total_products_ordered,
	ROUND(CAST(COUNT(DISTINCT fs.product_key) AS FLOAT)/COUNT(DISTINCT dp.product_key) * 100, 2) AS percent_products_ordered ,
	SUM(COALESCE(fs.price * fs.quantity, 0))/SUM(NULLIF(quantity, 0)) AS weighted_avg_price,
	COUNT(DISTINCT fs.customer_key) AS total_customers,
	COUNT(DISTINCT fs.order_number) AS total_orders,
	SUM(COALESCE(fs.quantity, 0)) AS total_quantity,
	SUM(COALESCE(fs.sales, 0)) AS total_sales
FROM gold.vw_dim_products dp
LEFT JOIN gold.vw_fact_sales fs
ON dp.product_key = fs.product_key
WHERE product_line != 'Unknown' AND category IS NOT NULL
GROUP BY dp.product_line, dp.category, dp.subcategory, dp.product_name
ORDER BY dp.product_line, dp.category, total_sales DESC;
