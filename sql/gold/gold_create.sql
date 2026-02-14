/*
File Name   : gold_create.sql
Layer       : Gold
Purpose     : Define analytics-ready dimension and fact tables.
Description :
- Creates Gold layer tables modeled for reporting and BI consumption.
- Includes dimension tables for descriptive attributes and fact tables for metrics.
- Tables follow star-schema design principles.

Notes       :
- Data is sourced from the Silver layer.
- Tables are recreated to support repeatable pipeline execution.
- No data is loaded in this script.
*/

IF OBJECT_ID ('gold.dim_customers', 'U') IS NOT NULL
	DROP TABLE gold.dim_customers;

-- Customer dimension with surrogate key for analytical joins

CREATE TABLE gold.dim_customers
(
	customer_key INT IDENTITY(1,1),
	customer_id VARCHAR(64) NOT NULL,
	customer_unique_id VARCHAR(64) NOT NULL,
	customer_city NVARCHAR(255),
	customer_state VARCHAR(5)
	CONSTRAINT pk_dim_customer PRIMARY KEY (customer_key),
	CONSTRAINT uk_dim_customer UNIQUE (customer_id)
);

IF OBJECT_ID ('gold.dim_product', 'U') IS NOT NULL
	DROP TABLE gold.dim_product;

-- Product dimension containing physical attributes only

CREATE TABLE gold.dim_product
(
	product_key INT IDENTITY(1,1),
	product_id VARCHAR(50) NOT NULL,
	product_category_name_english NVARCHAR(50),
	product_weight_g INT,
	product_length_cm INT,
	product_height_cm INT,
	product_width_cm INT
	CONSTRAINT pk_dim_product PRIMARY KEY (product_key),
	CONSTRAINT uk_dim_product UNIQUE (product_id)
);

IF OBJECT_ID ('gold.dim_sellers', 'U') IS NOT NULL
	DROP TABLE gold.dim_sellers;

-- Seller dimension representing merchant master data

CREATE TABLE gold.dim_sellers
(
	seller_key INT IDENTITY(1,1),
	seller_id VARCHAR(50),
	seller_city NVARCHAR(50),
	seller_state NVARCHAR(50)
	CONSTRAINT pk_dim_sellers PRIMARY KEY (seller_key),
	CONSTRAINT uk_dim_sellers UNIQUE (seller_id)
);

IF OBJECT_ID ('gold.dim_date', 'U') IS NOT NULL
	DROP TABLE gold.dim_date;

-- Date dimension supporting calendar and fiscal analysis

CREATE TABLE gold.dim_date
(
	date_key INT NOT NULL,
	full_date DATE NOT NULL,
	day_number_in_month INT NOT NULL,
	day_number_in_year INT NOT NULL,
	day_name VARCHAR(15) NOT NULL,
	day_of_week INT NOT NULL,
	week_of_year INT NOT NULL,
	month_number INT NOT NULL,
	month_name VARCHAR(15) NOT NULL,
	quarter_number INT NOT NULL,
	year_number INT NOT NULL,
	is_weekend INT NOT NULL,
	fiscal_month_number INT NOT NULL,
	fiscal_quarter INT NOT NULL,
	fiscal_year INT NOT NULL,
	first_day_of_month DATE NOT NULL,
	last_day_of_month DATE NOT NULL,
	first_day_of_year DATE NOT NULL,
	last_day_of_year DATE NOT NULL,
	CONSTRAINT pk_dim_date PRIMARY KEY(date_key),
	CONSTRAINT uk_dim_date UNIQUE(full_date)
);

IF OBJECT_ID ('gold.dim_customer_orders', 'U') IS NOT NULL
	DROP TABLE gold.dim_customer_orders;

-- Bridge dimension linking customers and orders for fact tables

CREATE TABLE gold.dim_customer_orders
(
	order_key INT IDENTITY(1,1),
	order_id VARCHAR(50) NOT NULL,
	customer_id VARCHAR(64) NOT NULL,
	customer_key INT NOT NULL,
	order_status VARCHAR(20),
	order_date_key INT,
	order_delivered_customer_date_key INT NOT NULL
	CONSTRAINT pk_dim_customer_orders PRIMARY KEY(order_key),
	CONSTRAINT uk_dim_customer_orders UNIQUE(order_id)
);

IF OBJECT_ID ('gold.fact_orders', 'U') IS NOT NULL
	DROP TABLE gold.fact_orders;

-- Order-level fact table with delivery lifecycle quality indicators

CREATE TABLE gold.fact_orders
(
	order_key INT NOT NULL,
	order_id VARCHAR(50) NOT NULL,
	customer_key INT NOT NULL,
	customer_id VARCHAR(64) NOT NULL,
	order_purchase_date_key INT NOT NULL,
	order_approved_date_key INT NOT NULL,
	order_delivered_carrier_date_key INT NOT NULL,
	order_delivered_customer_date_key INT NOT NULL,
	order_estimated_delivery_date_key INT NOT NULL,
	order_status VARCHAR(20),
	is_approval_date_null_invalid INT,
	is_delivered_carrier_date_null_invalid INT,
	is_delivered_customer_date_null_invalid INT,
	is_delivered_carrier_date_lessthan_approval_date INT,
	is_delivered_customer_date_lessthan_delivered_carrier_date INT,
	delivery_days INT,
	carrier_to_customer_days INT
	CONSTRAINT pk_fact_orders PRIMARY KEY (order_key),
	CONSTRAINT uk_fact_orders UNIQUE (order_id)
);

IF OBJECT_ID ('gold.fact_sales', 'U') IS NOT NULL
	DROP TABLE gold.fact_sales;

-- Sales fact table at order-item grain

CREATE TABLE gold.fact_sales
(
	sales_key INT IDENTITY(1,1) NOT NULL,
	order_key INT NOT NULL,
	order_id VARCHAR(50) NOT NULL,
	order_item_id INT NOT NULL,
	customer_key INT NOT NULL,
	product_key INT NOT NULL,
	seller_key INT NOT NULL,
	order_date_key INT NOT NULL,
	shipping_limit_date_key INT NOT NULL,
	order_status VARCHAR(20),
	price DECIMAL(8,2),
	freight_value DECIMAL(5,2),
	gross_amount DECIMAL(8,2)
	CONSTRAINT pk_fact_sales PRIMARY KEY(sales_key)
);

IF OBJECT_ID ('gold.fact_payments', 'U') IS NOT NULL
	DROP TABLE gold.fact_payments;

-- Payment fact table capturing transaction-level payment details

CREATE TABLE gold.fact_payments
(
	payment_key INT IDENTITY(1,1) NOT NULL,
	order_id VARCHAR(50) NOT NULL,
	payment_sequential INT NOT NULL,
	customer_key INT NOT NULL,
	order_date_key INT NOT NULL,
	payment_type VARCHAR(25),
	payment_installments INT,
	payment_value DECIMAL(8,2),
	is_zero_installments_with_value INT
	CONSTRAINT pk_fact_payments PRIMARY KEY(payment_key)
);

IF OBJECT_ID ('gold.fact_reviews', 'U') IS NOT NULL
	DROP TABLE gold.fact_reviews;

-- Review fact table linking customer feedback to products and sellers

CREATE TABLE gold.fact_reviews
(
	review_key INT IDENTITY(1,1) NOT NULL,
	review_id VARCHAR(50) NOT NULL,
	order_id VARCHAR(50) NOT NULL,
	order_key INT NOT NULL,
	customer_key INT NOT NULL,
	product_key INT NOT NULL,
	seller_key INT NOT NULL,
	review_date_key INT NOT NULL,
	review_answer_date_key INT NOT NULL,
	review_score INT,
	has_review_comment INT
	CONSTRAINT pk_fact_reviews PRIMARY KEY (review_key)
);

-- Shipping performance fact table capturing delivery timelines and status

IF OBJECT_ID ('gold.fact_shipping', 'U') IS NOT NULL
	DROP TABLE gold.fact_shipping;

CREATE TABLE gold.fact_shipping
(
	shipping_key INT IDENTITY(1,1) NOT NULL,
	order_id VARCHAR(50) NOT NULL,
	customer_key INT NOT NULL,
	order_key INT NOT NULL,
	order_purchase_date_key INT NOT NULL,
	order_approved_date_key INT NOT NULL,
	order_delivered_carrier_date_key INT NOT NULL,
	order_delivered_customer_date_key INT NOT NULL,
	order_estimated_delivery_date_key INT NOT NULL,
	days_to_approve INT,
	days_to_ship INT,
	days_in_transit INT,
	total_delivery_days INT,
	delivery_delay_days INT,
	delivery_status VARCHAR(15)
	CONSTRAINT pk_fact_shipping PRIMARY KEY (shipping_key)
);




