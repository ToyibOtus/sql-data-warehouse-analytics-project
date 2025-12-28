/*
==========================================================================
Part-to-Whole Analysis
==========================================================================
Script Purpose:
	This script performs part-to-whole analysis, highlighting the 
	percentage distribution of key business metrics across relevant
	dimensions.
==========================================================================
*/
-- Which country contributes the hiighest to the total revenue?
WITH metric_by_country AS
(
SELECT
	dc.country,
	COUNT(DISTINCT fs.order_number) AS orders_by_country,
	SUM(fs.quantity) AS quantity_by_country,
	SUM(fs.sales) AS sales_by_country
FROM gold.vw_fact_sales fs 
LEFT JOIN gold.vw_dim_customers dc
ON fs.customer_key = dc.customer_key
GROUP BY dc.country
)
, total_metric AS
(
SELECT
	country,
	SUM(orders_by_country) OVER() AS total_orders,
	SUM(quantity_by_country) OVER() AS total_quantity,
	SUM(sales_by_country) OVER() AS total_sales
FROM metric_by_country
)
SELECT
	mbc.country,
	ROUND(CAST(mbc.orders_by_country AS FLOAT)/tm.total_orders * 100, 2) AS percent_dist_orders,
	ROUND(CAST(mbc.quantity_by_country AS FLOAT)/tm.total_quantity * 100, 2) AS percent_dist_quantity,
	ROUND(CAST(mbc.sales_by_country AS FLOAT)/tm.total_sales * 100, 2) AS percent_dist_sales
FROM metric_by_country mbc
INNER JOIN total_metric tm
ON mbc.country = tm.country
ORDER BY percent_dist_sales DESC;

-- Do females bring in more revenue that males?
WITH metric_by_gender AS
(
SELECT
	dc.gender,
	COUNT(DISTINCT fs.order_number) AS orders_by_gender,
	SUM(fs.quantity) AS quantity_by_gender,
	SUM(fs.sales) AS sales_by_gender
FROM gold.vw_fact_sales fs 
LEFT JOIN gold.vw_dim_customers dc
ON fs.customer_key = dc.customer_key
GROUP BY dc.gender
)
, total_metric AS
(
SELECT
	gender,
	SUM(orders_by_gender) OVER() AS total_orders,
	SUM(quantity_by_gender) OVER() AS total_quantity,
	SUM(sales_by_gender) OVER() AS total_sales
FROM metric_by_gender
)
SELECT
	mbg.gender,
	ROUND(CAST(mbg.orders_by_gender AS FLOAT)/tm.total_orders * 100, 2) AS percent_dist_orders,
	ROUND(CAST(mbg.quantity_by_gender AS FLOAT)/tm.total_quantity * 100, 2) AS percent_dist_quantity,
	ROUND(CAST(mbg.sales_by_gender AS FLOAT)/tm.total_sales * 100, 2) AS percent_dist_sales
FROM metric_by_gender mbg
INNER JOIN total_metric tm
ON mbg.gender = tm.gender
WHERE mbg.gender != 'Unknown'
ORDER BY percent_dist_sales DESC;

-- Do married customers contribute more to the total revenue?
WITH metric_by_marital_status AS
(
SELECT
	dc.marital_status,
	COUNT(DISTINCT fs.order_number) AS orders_by_marital_status,
	SUM(fs.quantity) AS quantity_by_marital_status,
	SUM(fs.sales) AS sales_by_marital_status
FROM gold.vw_fact_sales fs 
LEFT JOIN gold.vw_dim_customers dc
ON fs.customer_key = dc.customer_key
GROUP BY dc.marital_status
)
, total_metric AS
(
SELECT
	marital_status,
	SUM(orders_by_marital_status) OVER() AS total_orders,
	SUM(quantity_by_marital_status) OVER() AS total_quantity,
	SUM(sales_by_marital_status) OVER() AS total_sales
FROM metric_by_marital_status
)
SELECT
	mbm.marital_status,
	ROUND(CAST(mbm.orders_by_marital_status AS FLOAT)/tm.total_orders * 100, 2) AS percent_dist_orders,
	ROUND(CAST(mbm.quantity_by_marital_status AS FLOAT)/tm.total_quantity * 100, 2) AS percent_dist_quantity,
	ROUND(CAST(mbm.sales_by_marital_status AS FLOAT)/tm.total_sales * 100, 2) AS percent_dist_sales
FROM metric_by_marital_status mbm
INNER JOIN total_metric tm
ON mbm.marital_status = tm.marital_status
WHERE mbm.marital_status != 'Unknown'
ORDER BY percent_dist_sales DESC;

-- Which of our customers contribute highest to the total revenue?
WITH metric_by_customer AS
(
SELECT
	dc.customer_key,
	dc.first_name,
	dc.last_name,
	COUNT(DISTINCT fs.order_number) AS orders_by_customer,
	SUM(fs.quantity) AS quantity_by_customer,
	SUM(fs.sales) AS sales_by_customer
FROM gold.vw_fact_sales fs 
LEFT JOIN gold.vw_dim_customers dc
ON fs.customer_key = dc.customer_key
GROUP BY
	dc.customer_key,
	dc.first_name,
	dc.last_name
)
, total_metric AS
(
SELECT
	customer_key,
	first_name,
	last_name,
	SUM(orders_by_customer) OVER() AS total_orders,
	SUM(quantity_by_customer) OVER() AS total_quantity,
	SUM(sales_by_customer) OVER() AS total_sales
FROM metric_by_customer
)
SELECT
	mbc.first_name,
	mbc.last_name,
	ROUND(CAST(mbc.orders_by_customer AS FLOAT)/tm.total_orders * 100, 2) AS percent_dist_orders,
	ROUND(CAST(mbc.quantity_by_customer AS FLOAT)/tm.total_quantity * 100, 2) AS percent_dist_quantity,
	ROUND(CAST(mbc.sales_by_customer AS FLOAT)/tm.total_sales * 100, 2) AS percent_dist_sales
FROM metric_by_customer mbc
INNER JOIN total_metric tm
ON mbc.customer_key = tm.customer_key
ORDER BY percent_dist_sales DESC;

-- Which of our product contributes highest to the total_revenue?
WITH metric_by_product AS
(
SELECT
	dp.product_key,
	dp.product_name,
	COUNT(DISTINCT fs.order_number) AS orders_by_product,
	SUM(fs.quantity) AS quantity_by_product,
	SUM(fs.sales) AS sales_by_product
FROM gold.vw_fact_sales fs 
LEFT JOIN gold.vw_dim_products dp
ON fs.product_key = dp.product_key
GROUP BY
	dp.product_key,
	dp.product_name
)
, total_metric AS
(
SELECT
	product_key,
	product_name,
	SUM(orders_by_product) OVER() AS total_orders,
	SUM(quantity_by_product) OVER() AS total_quantity,
	SUM(sales_by_product) OVER() AS total_sales
FROM metric_by_product
)
SELECT
	mbp.product_name,
	ROUND(CAST(mbp.orders_by_product AS FLOAT)/tm.total_orders * 100, 2) AS percent_dist_orders,
	ROUND(CAST(mbp.quantity_by_product AS FLOAT)/tm.total_quantity * 100, 2) AS percent_dist_quantity,
	ROUND(CAST(mbp.sales_by_product AS FLOAT)/tm.total_sales * 100, 2) AS percent_dist_sales
FROM metric_by_product mbp
INNER JOIN total_metric tm
ON mbp.product_key = tm.product_key
ORDER BY percent_dist_sales DESC;
