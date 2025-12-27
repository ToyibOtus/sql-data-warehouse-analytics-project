/*
========================================================================
Measure Exploration
========================================================================
Script Purpose:
	This script retrieves key business metrics, giving an overview of 
	the overall performance of our business.
========================================================================
*/
-- What is the total revenue generated?
SELECT SUM(sales) AS total_sales FROM gold.vw_fact_sales;

-- What is the total quantity of products sold
SELECT SUM(quantity) AS total_quantity FROM gold.vw_fact_sales;

-- What is the total number of orders had?
SELECT COUNT(DISTINCT order_number) AS total_orders FROM gold.vw_fact_sales;

-- What is the avg selling price?
SELECT avg(price) AS avg_selling_price FROM gold.vw_fact_sales;

-- How many customers do we have?
SELECT COUNT(customer_key) AS total_customers FROM gold.vw_dim_customers;

-- How many of these customers have ordered?
SELECT COUNT(DISTINCT customer_key) AS total_customers_ordered FROM gold.vw_fact_sales;

-- How many distinct products do we have?
SELECT COUNT(product_key) AS total_products FROM gold.vw_dim_products;

-- How many of these products have been purchased
SELECT COUNT(DISTINCT product_key) AS total_products_ordered FROM gold.vw_fact_sales;

-- What is our highest sale?
SELECT MAX(sales) AS highest_sales FROM gold.vw_fact_sales;

-- How does the lowest sale compare to our highest sale?
SELECT MIN(sales) AS lowest_sales FROM gold.vw_fact_sales;

-- How much do we make on average?
SELECT AVG(sales) AS avg_sales FROM gold.vw_fact_sales;

-- What is the higest amount of product ordered?
SELECT MAX(quantity) AS highest_quantity FROM gold.vw_fact_sales;

-- How does it compare with the lowest quantity of product ordered?
SELECT MIN(quantity) AS lowest_quantity FROM gold.vw_fact_sales;

-- How many products do we sell on average?
SELECT AVG(quantity) AS avg_quantity FROM gold.vw_fact_sales;



-- Report showing an overview of the business performance
SELECT 'Total Sales' AS measure_name, SUM(sales) AS measure_value FROM gold.vw_fact_sales
UNION ALL
SELECT 'Total Quantity', SUM(quantity) FROM gold.vw_fact_sales
UNION ALL
SELECT 'Total Orders', COUNT(DISTINCT order_number) FROM gold.vw_fact_sales
UNION ALL
SELECT 'Avg Selling Price', avg(price) FROM gold.vw_fact_sales
UNION ALL
SELECT 'Total Customers', COUNT(customer_key) FROM gold.vw_dim_customers
UNION ALL
SELECT 'Total Customers Ordered', COUNT(DISTINCT customer_key) FROM gold.vw_fact_sales
UNION ALL
SELECT 'Total Products', COUNT(product_key) FROM gold.vw_dim_products
UNION ALL
SELECT 'Total Products Ordered', COUNT(DISTINCT product_key) FROM gold.vw_fact_sales
UNION ALL
SELECT 'Highest Sales', MAX(sales) FROM gold.vw_fact_sales
UNION ALL
SELECT 'Lowest Sales', MIN(sales) FROM gold.vw_fact_sales
UNION ALL
SELECT 'Avg Sales', AVG(sales) FROM gold.vw_fact_sales
UNION ALL
SELECT 'Highest Quantity', MAX(quantity) FROM gold.vw_fact_sales
UNION ALL
SELECT 'Lowest Quantity', MIN(quantity) FROM gold.vw_fact_sales
UNION ALL
SELECT 'Avg Quantity', AVG(quantity) FROM gold.vw_fact_sales;
