/*
==========================================================================
Performance Analysis
==========================================================================
Script Purpose: 
	This script conducts a performance analysis. It digs deeper into the
	dataset, retrieving valuable data that shows insight into the 
	business performance.
==========================================================================
*/
-- Do most of our product generate revenue greater than their yearly average sales?
SELECT
	order_date_year,
	product_name,
	total_sales,
	avg_sales,
	sales_diff,
	percent_sales_diff,
	CASE	
		WHEN percent_sales_diff > 0 THEN 'Above Average'
		WHEN percent_sales_diff < 0 THEN 'Below Average'
		ELSE 'Equal to AVerage'
	END AS sales_status
FROM
(
SELECT
	order_date_year,
	product_key,
	product_name,
	total_sales,
	avg_sales,
	total_sales - avg_sales AS sales_diff,
	ROUND(CAST((total_sales - avg_sales) AS FLOAT)/(avg_sales) * 100, 2) AS percent_sales_diff
FROM
	(
		SELECT
			YEAR(fs.order_date) AS order_date_year,
			dp.product_key,
			dp.product_name,
			SUM(fs.sales) AS total_sales,
			AVG(SUM(fs.sales)) OVER(PARTITION BY dp.product_key) AS avg_sales
		FROM gold.vw_fact_sales fs
		LEFT JOIN gold.vw_dim_products dp
		ON fs.product_key = dp.product_key
		WHERE YEAR(fs.order_date) IS NOT NULL
		GROUP BY 
			YEAR(fs.order_date),
			dp.product_key,
			dp.product_name
	)SUB1
)SUB2
ORDER BY product_name, order_date_year;

-- Are most of our products generating more revenue with each passing year?
SELECT
	order_date_year,
	product_key,
	product_name,
	current_yr_sales,
	previous_yr_sales,
	sales_diff,
	percent_sales_diff,
	CASE	
		WHEN percent_sales_diff > 0 THEN 'Above Previous Sales'
		WHEN percent_sales_diff < 0 THEN 'Below Previous Sales'
		WHEN percent_sales_diff = 0 THEN 'Equal to Previous Sales'
		ELSE NULL
	END AS current_sales_status
FROM
(
SELECT
	order_date_year,
	product_key,
	product_name,
	current_yr_sales,
	previous_yr_sales,
	current_yr_sales - previous_yr_sales AS sales_diff,
	ROUND(CAST((current_yr_sales - previous_yr_sales) AS FLOAT)/(previous_yr_sales) * 100, 2) AS percent_sales_diff
FROM
	(
		SELECT
			YEAR(fs.order_date) AS order_date_year,
			dp.product_key,
			dp.product_name,
			SUM(fs.sales) AS current_yr_sales,
			LAG(SUM(fs.sales)) OVER(PARTITION BY dp.product_key ORDER BY YEAR(fs.order_date)) AS previous_yr_sales
		FROM gold.vw_fact_sales fs
		LEFT JOIN gold.vw_dim_products dp
		ON fs.product_key = dp.product_key
		WHERE YEAR(fs.order_date) IS NOT NULL
		GROUP BY 
			YEAR(fs.order_date),
			dp.product_key,
			dp.product_name
	)SUB1
)SUB2;

-- What year experienced the highest percentage increase in sales?
SELECT
	order_date_year,
	running_total_sales_current,
	LAG(running_total_sales_current) OVER(ORDER BY order_date_year) AS running_total_sales_previous,
	running_total_sales_current - LAG(running_total_sales_current) OVER(ORDER BY order_date_year) AS sales_diff,
	ROUND(CAST((running_total_sales_current - LAG(running_total_sales_current) OVER(ORDER BY order_date_year)) AS FLOAT)/
	LAG(running_total_sales_current) OVER(ORDER BY order_date_year) * 100, 2) AS percent_sales_increment
FROM
(
SELECT
	order_date_year,
	SUM(total_sales) OVER(ORDER BY order_date_year) AS running_total_sales_current
FROM
	(
		SELECT
			YEAR(order_date) AS order_date_year,
			SUM(sales) AS total_sales
		FROM gold.vw_fact_sales
		WHERE YEAR(order_date) IS NOT NULL
		GROUP BY YEAR(order_date)
	)SUB1
)SUB2;

-- What is our best month, and is it the same across the years? 
SELECT
	order_date_month,
	running_total_sales_current,
	LAG(running_total_sales_current) OVER(PARTITION BY YEAR(order_date_month) ORDER BY order_date_month) AS running_total_sales_previous,
	running_total_sales_current - LAG(running_total_sales_current) OVER(PARTITION BY YEAR(order_date_month) ORDER BY order_date_month) 
	AS sales_diff,
	ROUND(CAST((running_total_sales_current - LAG(running_total_sales_current) OVER(PARTITION BY YEAR(order_date_month) ORDER BY order_date_month)) AS FLOAT)/
	LAG(running_total_sales_current) OVER(PARTITION BY YEAR(order_date_month) ORDER BY order_date_month) * 100, 2) AS percent_sales_increment
FROM
(
SELECT
	order_date_month,
	SUM(total_sales) OVER(PARTITION BY YEAR(order_date_month) ORDER BY order_date_month) AS running_total_sales_current
FROM
	(
		SELECT
			DATETRUNC(month, order_date) AS order_date_month,
			SUM(sales) AS total_sales
		FROM gold.vw_fact_sales
		WHERE YEAR(order_date) IS NOT NULL
		GROUP BY DATETRUNC(month, order_date)
	)SUB1
)SUB2;

-- Are most of the yearly sales in close range with our highest yearly sales?
SELECT
	order_date_year,
	total_sales,
	MAX(total_sales) OVER() AS maximum_sales,
	total_sales - MAX(total_sales) OVER() AS sales_diff,
	ROUND(CAST(total_sales - MAX(total_sales) OVER() AS FLOAT)/MAX(total_sales) OVER() * 100, 2) AS percent_sales_diff
FROM
(
SELECT
	YEAR(order_date) AS order_date_year,
	SUM(sales) AS total_sales
FROM gold.vw_fact_sales
WHERE YEAR(order_date) IS NOT NULL
GROUP BY YEAR(order_date)
)SUB
ORDER BY order_date_year;

-- Are the monthly sales in close range with our best?
SELECT
	order_date_month,
	total_sales,
	MAX(total_sales) OVER(PARTITION BY YEAR(order_date_month)) AS maximum_sales,
	total_sales - MAX(total_sales) OVER(PARTITION BY YEAR(order_date_month)) AS sales_diff,
	ROUND(CAST(total_sales - MAX(total_sales) OVER(PARTITION BY YEAR(order_date_month)) AS FLOAT)
	/MAX(total_sales) OVER(PARTITION BY YEAR(order_date_month)) * 100, 2) AS percent_sales_diff
FROM
(
SELECT
	DATETRUNC(month, order_date) AS order_date_month,
	SUM(sales) AS total_sales
FROM gold.vw_fact_sales
WHERE YEAR(order_date) IS NOT NULL
GROUP BY DATETRUNC(month, order_date)
)SUB;
