# Data Dictionary for Gold Layer

## Overview
The **Gold Layer** is composed of business-ready data, structured to support analytical and reporting use cases. 
It consists of the following:

* **dimension tables**, and
* **fact tables**

These tables contain key business metrics that supports analysis, and thus enabling data-driven decisions.

---

### 1. gold.vw_dim_customers
* **Purpose:** Stores customer details, enriched with demographics and geographics data
* **Columns:**

|   **Column Name**  |   **Data Type**     |   **Description**                                                                         |
|--------------------|---------------------|-------------------------------------------------------------------------------------------|
|customer_key         |INT                 |Surrogate key uniquely identifying each customer record in the dimension table.|
|customer_id          |INT                 |Unique numerical identifier assigned to each customer.|
|customer_number      |NVARCHAR(50)        |Alphanumerical identifier representing each customer, used for tracking and referencing.|
|first_name           |NVARCHAR(50)        |The customer's first name recorded in the system.|
|last_name            |NVARCHAR(50)        |The customer's last name recorded in the system.|
|country              |NVARCHAR(50)        |The customer's country of residence (e.g., 'Australia').|
|marital_status       |NVARCHAR(50)        |The marital status of the customer (e.g., 'Married', 'Single', 'N/A').|
|gender               |NVARCHAR(50)        |The gender of the customer (e.g., 'Male', 'Female', 'N/A').|
|birth_date           |DATE                |The date of birth of the customer, formatted as YYYY-MM-DD (e.g., '1971-10-06').|
|create_date          |DATE                |The date the customer record was created in the system (e.g., '2025-10-06').|

---

### 2. gold.vw_dim_products
* **Purpose:** Provides detailed information about the products and their attributes
* **Columns:**

|   **Column Name**  |   **Data Type**     |   **Description**                                                                         |
|--------------------|---------------------|-------------------------------------------------------------------------------------------|
|product_key         |INT                  |Surrogate key uniquely identifying each product record in the dimension table.|
|product_id          |INT                  |Unique numerical identifier assigned to each product.|
|product_number      |NVARCHAR(50)         |Alphanumerical identifier representing each product, used for tracking and referencing.|
|product_name        |NVARCHAR(50)         |Descriptive name of the product, including key details such as type, colour, and size.|
|category_id         |NVARCHAR(50)         |A unique identifier for each product's category.|
|category            |NVARCHAR(50)         |A broader classification of products (e.g., 'Bike', 'Components').| 
|subcategory         |NVARCHAR(50)         |A more detailed classification of products (e.g., 'Road Frames', 'Mountain Bikes').|
|maintenance         |NVARCHAR(50)         |Indicates whether the product requires maintenance or not (e.g., 'Yes', 'No').|
|cost                |INT                  |The cost or base price of the product measured in monetary units.|
|product_line        |NVARCHAR(50)         |The specific product line or series to which the product belongs (e.g., 'Road', 'Mountain').|
|product_start_date  |DATE                 |The date the product became available for sale|

---

### 3. gold.vw_fact_sales
* **Purpose:** Stores transactional sales data for analytical purposes
* **Columns:**

|   **Column Name**  |   **Data Type**     |   **Description**                                                                         |
|--------------------|---------------------|-------------------------------------------------------------------------------------------|
|order_number        |NVARCHAR(50)         |A unque alphanumerical identifier assigned to each sales order (e.g., 'SO54496').|
|product_key         |INT                  |Surrogate key, linking **fact_sales** to **dim_products**.|
|customer_key        |INT                  |Surrogate key, linking **fact_sales** to **dim_customers**.| 
|order_date          |DATE                 |The date an order was placed.|
|shipping_date       |DATE                 |The date the product was shipped.|
|due_date            |DATE                 |The date the product is expected to be delivered.|
|sales               |INT                  |The total monetary value of the sale for the line item, in whole currency units (e.g., 25).|
|quantity            |INT                  |The number of units of the product ordered for the line item (e.g., 1).|
|price               |INT                  |The price per unit of the product for the line item, in whole currency units (e.g., 25).|

---

### 4. gold.vw_report_customers
* **Purpose:** Holds data prepared for visualization (BI tools)
* **Columns:**

|   **Column Name**  |   **Data Type**     |   **Description**                                                                         |
|--------------------|---------------------|-------------------------------------------------------------------------------------------|
|customer_key        |INT                  |A surrogate key unquely identifying each customer record in the report.
|customer_id          |INT                 |Unique numerical identifier assigned to each customer.|
|customer_number     |NVARCHAR(50)         |An alphanumerical unique identifier representing each customer, used for tracking and referencing.
|customer_name       |NVARCHAR(101)        |The customer's fullname recorded in the system.
|country             |NVARCHAR(50)         |The customer's country (e.g., 'Australia').
|gender              |NVARCHAR(50)         |The customer's gender (e.g., 'Male', 'Female', 'N/A').
|marital_status      |NVARCHAR(50)         |The customer's marital status (e.g., 'Single', 'Married', 'N/A').
|birth_date          |DATE                 |The birth date of each customer (e.g., '1971-10-06').
|age                 |INT                  |The customer's age (e.g., '54').
|age_group           |VARCHAR(8)           |The customer's age group (e.g., 'Below 20', '20-29', '30-39').
|customer_status     |VARCHAR(7)           |The status of each customer, representing both value and loyalty (e.g., 'VIP', 'Regular', & 'New').
|create_date          |DATE                |The date the customer record was created in the system (e.g., '2025-10-06').|
|first_order_date    |DATE                 |The customer's first order date (e.g., '2011-01-19').
|last_order_date     |DATE                 |The customer's last order date (e.g., '2013-05-03').
|lifespan_month      |INT                  |The months of history each customer has with the organization. 
|total_orders        |INT                  |The total number of orders made by each customer.
|total_quantity      |INT                  |The total quantity of products purchased by each customer.
|total_sales         |INT                  |The total amount of revenue each customer has generated.
|total_products      |INT                  |The unique number of products purchased by each customer.
|recency_month       |INT                  |The number of months since last order (e.g., '150').
|avg_order_value     |INT                  |The value of one order made by a customer (total_sales/total_orders).
|avg_monthly_spend   |INT                  |The average monthly spend (total_sales/lifespan_month).

### 5. gold.vw_report_products
* **Purpose:** Holds data prepared for visualization (BI tools)
* **Columns:**

|   **Column Name**  |   **Data Type**     |   **Description**                                                                         |
|--------------------|---------------------|-------------------------------------------------------------------------------------------|
|product_key         |INT                  |A surrogate key uniquely identifying each record in the report.
|product_id          |INT                  |Unique numerical identifier assigned to each product.|
|product_number      |NVARCHAR(50)         |An alphanumerical unique identifier assigned to each product, used for tracking and referencing
|product_name        |NVARCHAR(50)         |The product name recorded in the system (e.g., 'Road-150 Red- 62'). 
|product_line        |NVARCHAR(50)         |The product line each product falls into (e.g., 'Mountain', 'Road', 'Other Sales', and 'Touring').
|category_id         |NVARCHAR(50)         |A unique identifier for each product's category.
|category            |NVARCHAR(50)         |A broader classification of products (e.g., 'Bike', 'Components').
|subcategory         |NVARCHAR(50)         |A more detailed classification of products (e.g., 'Road Frames', 'Mountain Bikes').
|maintenance         |NVARCHAR(50)         |Indicates whether the product requires maintenance or not (e.g., 'Yes', 'No').
|product_cost        |INT                  |The cost or base price of the product measured in monetary units.
|avg_selling_price   |INT                  |The average selling price of each product.
|product_status      |VARCHAR(14)          |The status of each product (e.g., 'High Performer', 'Mid Range', 'Low Performer').
|product_start_date  |DATE                 |The date the product became available for sale|
|last_order_date     |DATE                 |The order date each product was last ordered.
|lifespan_month      |INT                  |The months of history each product has with the organization.
|total_orders        |INT                  |The total number of times a product was ordered.
|total_quantity      |INT                  |The total quantity of products purchased.
|total_sales         |INT                  |The total revenue generated by each product.
|total_customers     |INT                  |The unique number of customers that purchased each product.
|recency_month       |INT                  |The number of months since each product was last ordered.
|avg_order_revenue   |INT                  |The revenue generated per order (total_sales/total_orders).
|avg_monthly_revenue |INT                  |The revenue generated per month (total_sales/lifespan_month).

