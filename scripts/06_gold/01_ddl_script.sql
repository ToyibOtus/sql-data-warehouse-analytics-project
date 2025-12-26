/*
=========================================================================================================
DDL Script: Build Gold Tables
=========================================================================================================
Script Purpose:
	This script builds dimension and fact tables in the gold layer.
	Run this script to redefine the structure of your dimension and fact tables.
=========================================================================================================
*/
CREATE TABLE gold.dim_customers
(
	customer_key INT IDENTITY(1, 1) NOT NULL,
	customer_id INT NOT NULL,
	customer_number NVARCHAR(50) NOT NULL,
	first_name NVARCHAR(50) NOT NULL,
	last_name NVARCHAR(50) NOT NULL,
	country NVARCHAR(50),
	gender NVARCHAR(50) NOT NULL,
	marital_status NVARCHAR(50) NOT NULL,
	birth_date DATE,
	create_date DATE NOT NULL,
	dwh_step_run_id UNIQUEIDENTIFIER NOT NULL,
	dwh_raw_row NVARCHAR(MAX) NOT NULL,
	dwh_row_hash VARBINARY(32) NOT NULL,
	dwh_create_date DATETIME DEFAULT GETDATE(),
	CONSTRAINT pk_dim_customers PRIMARY KEY (customer_key),
	CONSTRAINT uq_customer_id UNIQUE(customer_id),
	CONSTRAINT uq_customer_number UNIQUE(customer_number),
	CONSTRAINT chk_gender CHECK(gender IN ('Male', 'Female', 'Unknown')),
	CONSTRAINT chk_marital_status CHECK(marital_status IN ('Single', 'Married', 'Unknown'))
);

CREATE TABLE gold.dim_products
(
	product_key INT IDENTITY(1, 1) NOT NULL,
	product_id INT NOT NULL,
	product_number NVARCHAR(50) NOT NULL,
	product_name NVARCHAR(50) NOT NULL,
	product_line NVARCHAR(50) NOT NULL,
	category_id NVARCHAR(50) NOT NULL,
	category NVARCHAR(50),
	subcategory NVARCHAR(50),
	maintenance NVARCHAR(50),
	product_cost INT,
	product_start_date DATE,
	dwh_step_run_id UNIQUEIDENTIFIER NOT NULL,
	dwh_raw_row NVARCHAR(MAX) NOT NULL,
	dwh_row_hash VARBINARY(32) NOT NULL,
	dwh_create_date DATETIME DEFAULT GETDATE(),
	CONSTRAINT pk_dim_products PRIMARY KEY (product_key),
	CONSTRAINT uq_product_id UNIQUE(product_id),
	CONSTRAINT uq_product_number UNIQUE(product_number),
	CONSTRAINT uq_product_name UNIQUE(product_name)
);

CREATE TABLE gold.fact_sales
(
	order_number NVARCHAR(50) NOT NULL,
	product_key INT NOT NULL,
	customer_key INT NOT NULL,
	order_date DATE,
	shipping_date DATE,
	due_date DATE,
	sales INT NOT NULL,
	quantity INT NOT NULL,
	price INT NOT NULL,
	dwh_step_run_id UNIQUEIDENTIFIER NOT NULL,
	dwh_raw_row NVARCHAR(MAX) NOT NULL,
	dwh_row_hash VARBINARY(32) NOT NULL,
	dwh_create_date DATETIME DEFAULT GETDATE(),
	CONSTRAINT uq_order_number_product_key UNIQUE(order_number, product_key),
	CONSTRAINT fk_fact_sales_dim_products FOREIGN KEY(product_key) REFERENCES gold.dim_products(product_key),
	CONSTRAINT fk_fact_sales_key_dim_customers FOREIGN KEY(customer_key) REFERENCES gold.dim_customers(customer_key)
);
