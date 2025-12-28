/*
==========================================================================
Change Over Time Analysis
==========================================================================
Script Purpose: 
	This script performs year-over-year & month-over-month analyses,
	showing insight into how relevant business metrics change over time.
==========================================================================
*/
-- Year-Over-Year Analysis
SELECT
	YEAR(order_date) AS order_date_year,
	SUM(price * quantity)/SUM(quantity) AS weighted_avg_price,
	COUNT(DISTINCT customer_key) AS total_customers,
	COUNT(DISTINCT order_number) AS total_orders,
	COUNT(DISTINCT product_key) AS total_products,
	SUM(quantity) AS total_quantity,
	SUM(sales) AS total_sales
FROM gold.vw_fact_sales
WHERE YEAR(order_date) IS NOT NULL
GROUP BY YEAR(order_date);

-- Month-Over-Month Analysis
SELECT
	DATETRUNC(month, order_date) AS order_date_month,
	SUM(price * quantity)/SUM(quantity) AS weighted_avg_price,
	COUNT(DISTINCT customer_key) AS total_customers,
	COUNT(DISTINCT order_number) AS total_orders,
	COUNT(DISTINCT product_key) AS total_products,
	SUM(quantity) AS total_quantity,
	SUM(sales) AS total_sales
FROM gold.vw_fact_sales
WHERE YEAR(order_date) IS NOT NULL
GROUP BY DATETRUNC(month, order_date)
ORDER BY DATETRUNC(month, order_date);
