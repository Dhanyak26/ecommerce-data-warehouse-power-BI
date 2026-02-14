/*
File Name   : silver_create.sql
Layer       : Silver
Purpose     : Define cleaned and standardized tables for the e-commerce dataset.
Description :
- Creates silver-layer tables derived from bronze data.
- Tables are designed with consistent naming and data types.
- Supports downstream analytical and reporting use cases.

Notes       :
- No aggregations are defined at this stage.
- Business rules and data cleansing are applied during loading.
- Tables are recreated to support repeatable pipeline execution.
- Depends on bronze-layer tables.
*/

-- Assumption: primary keys from bronze layer remain stable after cleansing

IF OBJECT_ID ('silver.customer_info', 'U') IS NOT NULL
	DROP TABLE silver.customer_info;

CREATE TABLE silver.customer_info
(
	customer_id VARCHAR(64) NOT NULL,
	customer_unique_id VARCHAR(64) NOT NULL,
	customer_zip_code_prefix VARCHAR(10),
	customer_city NVARCHAR(255),
	customer_state VARCHAR(5)
	CONSTRAINT pk_silver_customer_info PRIMARY KEY (customer_id)
);

IF OBJECT_ID ('silver.geolocation', 'U') IS NOT NULL
	DROP TABLE silver.geolocation;

CREATE TABLE silver.geolocation
(
	geolocation_zip_code_prefix VARCHAR(10),
	geolocation_lat DECIMAL(9,6),
	geolocation_lng DECIMAL(9,6),
	geolocation_city NVARCHAR(50),
	geolocation_state NVARCHAR(50)
);
IF OBJECT_ID ('silver.order_items', 'U') IS NOT NULL
	DROP TABLE silver.order_items;

CREATE TABLE silver.order_items
(
	order_id VARCHAR(50) NOT NULL,
	order_item_id INT NOT NULL,
	product_id VARCHAR(50) NOT NULL,
	seller_id VARCHAR(50) NOT NULL,
	shipping_limit_date DATETIME2(0),
	price DECIMAL(8,2),
	freight_value DECIMAL(5,2)
	CONSTRAINT pk_silver_order_items PRIMARY KEY (order_id, order_item_id)
);
IF OBJECT_ID ('silver.order_payments', 'U') IS NOT NULL
	DROP TABLE silver.order_payments;

CREATE TABLE silver.order_payments
(
	order_id VARCHAR(50) NOT NULL,
	payment_sequential INT NOT NULL,
	payment_type VARCHAR(25),
	payment_installments INT,
	payment_value DECIMAL(8,2),
	is_zero_installments_with_value INT
	CONSTRAINT pk_silver_order_payments PRIMARY KEY (order_id, payment_sequential)
);
IF OBJECT_ID ('silver.order_reviews', 'U') IS NOT NULL
	DROP TABLE silver.order_reviews;

CREATE TABLE silver.order_reviews
(
	review_id VARCHAR(50) NOT NULL,
	order_id VARCHAR(50) NOT NULL,
	review_score INT,
	review_comment_message NVARCHAR(MAX),
	review_creation_date DATE,
	review_answer_timestamp DATETIME2(0)
	CONSTRAINT pk_silver_order_reviews PRIMARY KEY (order_id, review_id)
);

IF OBJECT_ID ('silver.orders', 'U') IS NOT NULL
	DROP TABLE silver.orders;

CREATE TABLE silver.orders
(
	order_id VARCHAR(50) NOT NULL,
	customer_id VARCHAR(50) NOT NULL,
	order_status VARCHAR(20),
	order_purchase_timestamp DATETIME2(0),
	order_approved_at DATETIME2(0),
	order_delivered_carrier_date DATETIME2(0),
	order_delivered_customer_date DATETIME2(0),
	order_estimated_delivery_date DATE,
	is_approval_date_null_invalid INT,
	is_delivered_carrier_date_null_invalid INT,
	is_delivered_customer_date_null_invalid INT,
	is_delivered_carrier_date_lessthan_approval_date INT,
	is_delivered_customer_date_lessthan_delivered_carrier_date INT
	CONSTRAINT pk_silver_orders PRIMARY KEY (order_id)
);

IF OBJECT_ID ('silver.products', 'U') IS NOT NULL
	DROP TABLE silver.products;

CREATE TABLE silver.products
(
	product_id VARCHAR(50) NOT NULL,
	product_category_name NVARCHAR(50),
	product_weight_g INT,
	product_length_cm INT,
	product_height_cm INT,
	product_width_cm INT,
	is_product_weight_missing INT,
	is_product_length_missing INT,
	is_product_height_missing INT,
	is_product_width_missing INT
	CONSTRAINT pk_silver_products PRIMARY KEY (product_id)
);

/*
-- Derived from bronze product data to capture content quality metrics
-- (text and media attributes), independent of physical product dimensions.
-- Used for analyzing catalog completeness and listing quality.
*/

IF OBJECT_ID ('silver.product_content_metrics', 'U') IS NOT NULL
	DROP TABLE silver.product_content_metrics;

CREATE TABLE silver.product_content_metrics
(
	product_id VARCHAR(50) NOT NULL,	
	product_name_lenght INT,
	product_description_lenght INT,
	product_photos_qty INT
	CONSTRAINT pk_silver_product_content_metrics PRIMARY KEY(product_id)
);

IF OBJECT_ID ('silver.sellers', 'U') IS NOT NULL
	DROP TABLE silver.sellers;

CREATE TABLE silver.sellers
(
	seller_id VARCHAR(50) NOT NULL,
	seller_zip_code_prefix VARCHAR(10),
	seller_city NVARCHAR(50),
	seller_state NVARCHAR(50)
	CONSTRAINT pk_silver_sellers PRIMARY KEY (seller_id)
);
IF OBJECT_ID ('silver.product_category_name_translation', 'U') IS NOT NULL
	DROP TABLE silver.product_category_name_translation;

CREATE TABLE silver.product_category_name_translation
(
	product_category_name NVARCHAR(50),
	product_category_name_english NVARCHAR(50)
)