/*
File Name      : silver_load.sql
Layer       : Silver
Purpose     : Clean, standardize, and load data into silver tables.
Description :
- Transforms raw bronze data into structured silver tables.
- Applies data quality rules and basic business logic.
- Produces reliable, analysis-ready datasets.

Notes       :
- Handles null values, duplicates, and invalid records.
- No aggregations are performed at this stage.
- Serves as the foundation for gold-layer modeling.
*/

-- Assumption: bronze tables contain raw but complete source data

-- ============================================
-- Clean and load customer master data
-- ============================================

INSERT INTO silver.customer_info
(
	customer_id,
	customer_unique_id,
	customer_zip_code_prefix,
	customer_city,
	customer_state
)
SELECT
	customer_id,
	customer_unique_id,
	customer_zip_code_prefix,
	customer_city,
	customer_state
FROM bronze.customer_info;

INSERT INTO silver.geolocation
(
	geolocation_zip_code_prefix,
	geolocation_lat,
	geolocation_lng,
	geolocation_city,
	geolocation_state
)
SELECT
	TRIM(REPLACE(geolocation_zip_code_prefix, '"', '')) AS geolocation_zip_code_prefix,
	geolocation_lat,
	geolocation_lng,
	LOWER(TRIM(TRANSLATE(REPLACE (geolocation_city,'  ',' '),'áàâãäéèêëíìîïóòôõöúùûüçñÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇÑ*".´','aaaaaeeeeiiiiooooouuuucnaaaaaeeeeiiiiooooouuuucn    '))) COLLATE Latin1_General_CI_AI AS geolocation_city,
	geolocation_state
FROM bronze.geolocation;

-- ============================================
-- Clean and load order items master data
-- ============================================

INSERT INTO silver.order_items
(
	order_id,
	order_item_id,
	product_id,
	seller_id,
	shipping_limit_date,
	price,
	freight_value
)
SELECT
	TRIM(REPLACE(order_id, '"', '')) AS order_id,
	order_item_id,
	TRIM(REPLACE(product_id, '"', '')) AS product_id,
	TRIM(REPLACE(seller_id, '"', '')) AS seller_id,
	CAST(shipping_limit_date AS DATETIME2(0)) AS shipping_limit_date,
	price,
	freight_value
FROM bronze.order_items;

-- ============================================
-- Clean and load order payments master data
-- ============================================

INSERT INTO silver.order_payments
(
	order_id,
	payment_sequential,
	payment_type,
	payment_installments,
	payment_value,
	is_zero_installments_with_value
)
SELECT
	order_id ,
	payment_sequential,
	payment_type,
	payment_value,
	payment_installments,
	CASE
		WHEN payment_installments = 0 AND payment_value > 0
		THEN 1 ELSE 0
	END AS is_zero_installments_with_value
FROM bronze.order_payments;

-- ============================================
-- Clean and load order reviews master data
-- ============================================

INSERT INTO silver.order_reviews
(
	review_id,
	order_id,
	review_score,
	review_comment_message,
	review_creation_date,
	review_answer_timestamp
)
SELECT
	review_id,
	order_id,
	review_score,
	LOWER(TRIM(TRANSLATE(review_comment_message,'áàâãäéèêëíìîïóòôõöúùûüçñÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇÑ','aaaaaeeeeiiiiooooouuuucnaaaaaeeeeiiiiooooouuuucn'))) COLLATE Latin1_General_CI_AI AS review_comment_message,
	CAST (review_creation_date AS DATE) AS review_creation_date,
	CAST (review_answer_timestamp AS DATETIME2(0)) AS review_answer_timestamp
FROM bronze.order_reviews;

-- ============================================
-- Clean and load orders master data
-- ============================================

INSERT INTO silver.orders
(
	order_id,
	customer_id,
	order_status,
	order_purchase_timestamp,
	order_approved_at,
	order_delivered_carrier_date,
	order_delivered_customer_date,
	order_estimated_delivery_date,
	is_approval_date_null_invalid,
	is_delivered_carrier_date_null_invalid,
	is_delivered_customer_date_null_invalid,
	is_delivered_carrier_date_lessthan_approval_date,
	is_delivered_customer_date_lessthan_delivered_carrier_date
)
SELECT
	order_id,
	customer_id,
	order_status,
	CAST(order_purchase_timestamp AS DATETIME2(0)) AS order_purchase_timestamp,
	CAST(order_approved_at AS DATETIME2(0)) AS order_approved_at,
	CAST( order_delivered_carrier_date AS DATETIME2(0)) AS  order_delivered_carrier_date,
	CAST(order_delivered_customer_date AS DATETIME2(0)) AS order_delivered_customer_date,
	CAST(order_estimated_delivery_date AS DATE) AS order_estimated_delivery_date,
/*
-- Data quality validation flags for delivered orders:
-- Identifies missing or logically inconsistent order lifecycle timestamps.
-- Flags are used for anomaly analysis and downstream filtering, not row exclusion.
*/
	CASE WHEN order_approved_at IS NULL AND order_status = 'delivered' THEN 1
		 ELSE 0
	END AS is_approval_date_null_invalid,
	CASE WHEN order_delivered_carrier_date IS NULL AND order_status = 'delivered' THEN 1
		 ELSE 0 
	END AS is_delivered_carrier_date_null_invalid,
	CASE WHEN order_delivered_customer_date IS NULL AND order_status = 'delivered' THEN 1
		 ELSE 0
	END AS is_delivered_customer_date_null_invalid,

-- Invalid sequence: delivery carrier date precedes order approval

	CASE WHEN order_delivered_carrier_date < order_approved_at THEN 1
		 ELSE 0 
	END AS is_delivered_carrier_date_lessthan_approval_date,

-- Invalid sequence: delivery customer date precedes delivery carrier date

	CASE WHEN order_delivered_customer_date < order_delivered_carrier_date THEN 1
		 ELSE 0
	END AS is_delivered_customer_date_lessthan_delivered_carrier_date
	FROM bronze.orders;

-- ============================================
-- Clean and load products master data
-- ============================================

INSERT INTO silver.products
(
	product_id,
	product_category_name,
	product_weight_g,
	product_length_cm,
	product_height_cm,
	product_width_cm,
	is_product_weight_missing,
	is_product_length_missing,
	is_product_height_missing,
	is_product_width_missing
)
SELECT
	product_id,
	CASE WHEN product_category_name IS NULL THEN 'unknown'
		 WHEN product_category_name LIKE '%_%' THEN REPLACE(product_category_name, '_',' ')
		 ELSE product_category_name
	END AS product_category_name,
/*
 Standardize invalid physical dimensions by converting non-positive values to NULL
 while retaining data quality indicators to track missing or unusable measurements.
*/
	CASE WHEN product_weight_g <= 0 THEN NULL
		 ELSE product_weight_g
	END AS product_weight_g,
	CASE WHEN product_length_cm <= 0 THEN NULL
		 ELSE product_length_cm
	END AS product_length_cm,
	CASE WHEN product_height_cm <= 0 THEN NULL
		 ELSE product_height_cm
	END AS product_height_cm,
	CASE WHEN product_width_cm <= 0 THEN NULL
		 ELSE product_width_cm
	END AS product_width_cm,
	CASE WHEN product_weight_g IS NULL OR product_weight_g <= 0 THEN 1
	ELSE 0
	END AS is_product_weight_missing,
	CASE WHEN product_length_cm IS NULL OR product_length_cm <= 0 THEN 1
	ELSE 0
	END AS is_product_length_missing,
	CASE WHEN product_height_cm IS NULL OR product_height_cm <= 0 THEN 1
	ELSE 0
	END AS is_product_height_missing,
	CASE WHEN product_width_cm IS NULL OR product_width_cm <= 0 THEN 1
	ELSE 0
	END AS is_product_width_missing
FROM bronze.products;

-- ======================================================
-- Clean and load product_content_metrics master data
-- ======================================================

INSERT INTO silver.product_content_metrics
(
	product_id,	
	product_name_lenght,
	product_description_lenght,
	product_photos_qty
)
SELECT
	product_id,	
	product_name_lenght,
	product_description_lenght,
	product_photos_qty
FROM bronze.products;

-- ======================================================
-- Clean and load sellers master data
-- ======================================================

INSERT INTO silver.sellers
(
	seller_id,
	seller_zip_code_prefix,
	seller_city,
	seller_state
)
SELECT
	seller_id,
	seller_zip_code_prefix,
	CASE WHEN seller_city = '04482255' OR seller_city = 'sbc' OR seller_city = 'sbc/sp' OR seller_city = 'sp' OR seller_city = 'sp / sp' OR seller_city = 'vendas@creditparts.com.br' THEN NULL
		 WHEN seller_city = 'cariacica / es' THEN 'cariacica'
		 WHEN seller_city = 'cascavael' THEN 'cascavel'
		 WHEN seller_city = 'balenario camboriu'  THEN 'balneario camboriu'
		 WHEN seller_city = 'lages - sc' THEN 'lages'
		 WHEN seller_city = 'mogi das cruzes / sp' THEN 'mogi das cruzes'
		 WHEN seller_city = 'pinhais/pr' THEN 'pinhais'
		 WHEN seller_city = 'rio de janeiro / rio de janeiro' OR seller_city = 'rio de janeiro \rio de janeiro' OR seller_city = 'rio de janeiro, rio de janeiro, brasil' THEN 'rio de janeiro'
		 WHEN seller_city = 'santa barbara d´oeste' OR seller_city = 'santa barbara d oeste' THEN 'santa barbara d''oeste'
		 WHEN seller_city = 'sao  paulo' OR seller_city = 'sao paulo' OR  seller_city = 'são paulo' OR seller_city = 'sao paulo - sp' OR seller_city = 'sao paulo / sao paulo' OR seller_city = 'sao paulo sp' OR seller_city = 'sao paulop' OR seller_city = 'sao pauo' THEN 'sao paluo'
		 ELSE seller_city
	END AS seller_city,
	seller_state
FROM bronze.sellers;

-- ==============================================================
-- Clean and load product_category_name_translation master data
-- ==============================================================


 INSERT INTO silver.product_category_name_translation
(
	product_category_name,
	product_category_name_english
)
SELECT
	REPLACE(product_category_name, '_',' ') AS product_category_name,
	REPLACE(product_category_name_english, '_',' ') AS product_category_name_english
FROM bronze.product_category_name_translation