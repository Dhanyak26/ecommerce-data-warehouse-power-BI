/*
File Name      : bronze_create.sql
Layer          : Bronze
Purpose        : Create raw staging tables for the e-commerce dataset.
Description    :
- This script defines the schema for bronze-layer tables.
- Data is stored in its raw form.
- Used as the first ingestion layer from source CSV files.

Notes          :
- No business logic applied.
- Data quality issues (nulls, duplicates) are handled in later layers.
- Tables are recreated only during initial setup.

*/

--Recreate table to allow repeatable pipeline setup

IF OBJECT_ID ('bronze.customer_info', 'U') IS NOT NULL
	DROP TABLE bronze.customer_info;

CREATE TABLE bronze.customer_info
(
-- Assumption: column data types match source CSV definitions

	customer_id VARCHAR(64),
	customer_unique_id VARCHAR(64),
	customer_zip_code_prefix VARCHAR(10),
	customer_city NVARCHAR(255),
	customer_state VARCHAR(5)
);

IF OBJECT_ID ('bronze.geolocation', 'U') IS NOT NULL
	DROP TABLE bronze.geolocation;

CREATE TABLE bronze.geolocation
(
	geolocation_zip_code_prefix VARCHAR(10),
	geolocation_lat DECIMAL(9,6),
	geolocation_lng DECIMAL(9,6),
	geolocation_city NVARCHAR(50),
	geolocation_state NVARCHAR(50)
);
IF OBJECT_ID ('bronze.order_items', 'U') IS NOT NULL
	DROP TABLE bronze.order_items;

CREATE TABLE bronze.order_items
(
	order_id VARCHAR(50),
	order_item_id INT,
	product_id VARCHAR(50),
	seller_id VARCHAR(50),
	shipping_limit_date DATETIME,
	price DECIMAL(8,2),
	freight_value DECIMAL(5,2)
);
IF OBJECT_ID ('bronze.order_payments', 'U') IS NOT NULL
	DROP TABLE bronze.order_payments;

CREATE TABLE bronze.order_payments
(
	order_id VARCHAR(50),
	payment_sequential INT,
	payment_type VARCHAR(25),
	payment_installments INT,
	payment_value DECIMAL(8,2)
);
IF OBJECT_ID ('bronze.order_reviews', 'U') IS NOT NULL
	DROP TABLE bronze.order_reviews;

CREATE TABLE bronze.order_reviews
(
	review_id VARCHAR(50),
	order_id VARCHAR(50),
	review_score INT,
	review_comment_title NVARCHAR(255),
	review_comment_message NVARCHAR(MAX),
	review_creation_date DATETIME,
	review_answer_timestamp DATETIME
);

IF OBJECT_ID ('bronze.orders', 'U') IS NOT NULL
	DROP TABLE bronze.orders;

CREATE TABLE bronze.orders
(
	order_id VARCHAR(50),
	customer_id VARCHAR(50),
	order_status VARCHAR(20),
	order_purchase_timestamp DATETIME,
	order_approved_at DATETIME,
	order_delivered_carrier_date DATETIME,
	order_delivered_customer_date DATETIME,
	order_estimated_delivery_date DATETIME
);

IF OBJECT_ID ('bronze.products', 'U') IS NOT NULL
	DROP TABLE bronze.products;

CREATE TABLE bronze.products
(
	product_id VARCHAR(50),
	product_category_name NVARCHAR(50),
	product_name_lenght INT,
	product_description_lenght INT,
	product_photos_qty INT,
	product_weight_g INT,
	product_length_cm INT,
	product_height_cm INT,
	product_width_cm INT
);

IF OBJECT_ID ('bronze.sellers', 'U') IS NOT NULL
	DROP TABLE bronze.sellers;

CREATE TABLE bronze.sellers
(
	seller_id VARCHAR(50),
	seller_zip_code_prefix VARCHAR(10),
	seller_city NVARCHAR(50),
	seller_state NVARCHAR(50)
);
IF OBJECT_ID ('bronze.product_category_name_translation', 'U') IS NOT NULL
	DROP TABLE bronze.product_category_name_translation;

CREATE TABLE bronze.product_category_name_translation
(
	product_category_name NVARCHAR(50),
	product_category_name_english NVARCHAR(50)

)