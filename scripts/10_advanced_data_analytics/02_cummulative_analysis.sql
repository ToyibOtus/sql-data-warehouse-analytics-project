/*
==========================================================================
Cummulative Analysis
==========================================================================
Script Purpose: 
	This script performs cummulative analysis, showing insight into the
	business progressiveness.
==========================================================================
*/
-- Year-Over-Year Analysis
SELECT
	order_date_year,
	SUM(weighted_price) OVER(ORDER BY order_date_year)
	/SUM(total_quantity) OVER(ORDER BY order_date_year) AS weighted_moving_avg,
	SUM(total_orders) OVER(ORDER BY order_date_year) AS running_total_orders,
	SUM(total_quantity) OVER(ORDER BY order_date_year) AS running_total_quantity,
	SUM(total_sales) OVER(ORDER BY order_date_year) AS running_total_sales
FROM
(
SELECT
	YEAR(order_date) AS order_date_year,
	SUM(price * quantity) AS weighted_price,
	COUNT(DISTINCT order_number) AS total_orders,
	SUM(quantity) AS total_quantity,
	SUM(sales) AS total_sales
FROM gold.vw_fact_sales
WHERE YEAR(order_date) IS NOT NULL
GROUP BY YEAR(order_date)
)SUB;

-- Month-Over-Month Analysis
SELECT
	order_date_month,
	SUM(weighted_price) OVER(PARTITION BY YEAR(order_date_month) ORDER BY order_date_month)
	/SUM(total_quantity)  OVER(PARTITION BY YEAR(order_date_month) ORDER BY order_date_month) AS weighted_moving_avg,
	SUM(total_orders) OVER(PARTITION BY YEAR(order_date_month) ORDER BY order_date_month) AS running_total_orders,
	SUM(total_quantity) OVER(PARTITION BY YEAR(order_date_month) ORDER BY order_date_month) AS running_total_quantity,
	SUM(total_sales) OVER(PARTITION BY YEAR(order_date_month) ORDER BY order_date_month) AS running_total_sales
FROM
(
SELECT
	DATETRUNC(month, order_date) AS order_date_month,
	SUM(price * quantity) AS weighted_price,
	COUNT(DISTINCT order_number) AS total_orders,
	SUM(quantity) AS total_quantity,
	SUM(sales) AS total_sales
FROM gold.vw_fact_sales
WHERE YEAR(order_date) IS NOT NULL
GROUP BY DATETRUNC(month, order_date)
)SUB;
