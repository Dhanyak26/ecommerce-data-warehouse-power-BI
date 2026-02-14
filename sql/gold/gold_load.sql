/*
File Name   : gold_load.sql
Layer       : Gold
Purpose     : Populate Gold layer dimension and fact tables.
Description :
- Loads transformed data from the Silver layer into Gold tables.
- Applies business logic, aggregations, and derived metrics.
- Produces final datasets for dashboards and analytical queries.

Notes       :
- Assumes Silver layer data is cleaned and standardized.
- Execution order matters due to table dependencies.
- Final output layer of the data pipeline.
*/

-- -- Populate customer dimension with unique customer identifiers and location details

INSERT INTO gold.dim_customers
(
	customer_id,
	customer_unique_id,
	customer_city,
	customer_state
)
SELECT 
	c.customer_id,
	c.customer_unique_id,
	c.customer_city,
	c.customer_state
FROM silver.customer_info c;

-- Populate product dimension with physical attributes and translated category names

INSERT INTO gold.dim_product
(
	product_id,
	product_category_name_english,
	product_weight_g,
	product_length_cm,
	product_height_cm,
	product_width_cm	
)
SELECT
	p.product_id,
	pt.product_category_name_english,
	p.product_weight_g,
	p.product_length_cm,
	p.product_height_cm,
	p.product_width_cm
FROM silver.products p 

-- Join to category translation table to expose English product category names

LEFT JOIN silver.product_category_name_translation pt
ON p.product_category_name = pt.product_category_name;

-- Populate seller dimension with seller identifiers and geographic details

INSERT INTO gold.dim_sellers
(
	seller_id,
	seller_city,
	seller_state
)
SELECT
	s.seller_id,
	s.seller_city,
	s.seller_state
FROM silver.sellers s;

-- Derive dynamic date range based on order purchase history for date dimension generation

DECLARE @start_date DATE, @end_date DATE;

-- Identify earliest purchase date and extend range by one year to support future analysis

SELECT
	@start_date = MIN(order_purchase_timestamp),
	@end_date = DATEADD(YEAR, 1, MAX(order_purchase_timestamp))
FROM silver.orders
WHERE order_purchase_timestamp IS NOT NULL;

;WITH calender AS 
(
	SELECT @start_date AS full_date
	UNION ALL
	SELECT DATEADD(DAY,1,full_date)
	FROM calender 
	WHERE full_date < @end_date
)

-- Generate date dimension covering full analytical time range

INSERT INTO gold.dim_date
(
	date_key,
	full_date,
	day_number_in_month,
	day_number_in_year,
	day_name,
	day_of_week,
	week_of_year,
	month_number,
	month_name,
	quarter_number,
	year_number,
	is_weekend,
	fiscal_month_number,
	fiscal_quarter,
	fiscal_year,
	first_day_of_month,
	last_day_of_month,
	first_day_of_year,
	last_day_of_year 
)
SELECT
	CONVERT(INT,FORMAT(full_date, 'yyyyMMdd')) AS date_key,
	full_date,
	DAY(full_date) AS day_number_in_month,

-- Derive standard calendar attributes for time-based reporting

	DATEPART(DAYOFYEAR, full_date) AS day_number_in_year,
	DATENAME(WEEKDAY,full_date) AS day_name,
	DATEPART(WEEKDAY,full_date) AS day_of_week,
	DATEPART(ISOWK, full_date) AS week_of_year,
	MONTH(full_date) AS month_number,
	DATENAME(MONTH, full_date) AS month_name,
	DATEPART(QUARTER, full_date) AS quarter_number,
	YEAR(full_date) AS year_number,
	CASE WHEN DATENAME(WEEKDAY, full_date) IN ('Saturday', 'Sunday') THEN 1
		 ELSE 0
	END AS is_weekend,

-- Generate calendar dimension with fiscal and calendar attributes
-- Fiscal month derived to align reporting with fiscal periods

	CASE WHEN MONTH(full_date) >= 4 THEN MONTH(full_date) - 3
		 ELSE MONTH(full_date) + 9
	END AS fiscal_month_number,

-- Fiscal quarter derived based on fiscal year definition

	CASE WHEN MONTH(full_date) IN (4,5,6) THEN 1
		 WHEN MONTH(full_date) IN (7,8,9) THEN 2
		 WHEN MONTH(full_date) IN (10,11,12) THEN 3
		 ELSE 4
	END AS fiscal_quarter,

-- Fiscal year derived assuming fiscal year starts in April

	CASE WHEN MONTH(full_date) >= 4 THEN YEAR(full_date)
		 ELSE YEAR(full_date) - 1
	END AS fiscal_year,
	DATEFROMPARTS(YEAR(full_date), MONTH(full_date),1) AS first_day_of_month,
	EOMONTH(full_date) AS last_day_of_month,
	DATEFROMPARTS(YEAR(full_date),1,1) AS first_day_of_year,
	DATEFROMPARTS(YEAR(full_date), 12, 31) AS last_day_of_year
FROM calender

-- Expand date range into individual calendar dates

OPTION (MAXRECURSION 32767);

-- Purpose: Load customer–order relationship with order lifecycle dates
-- Grain: One row per order_id

INSERT INTO gold.dim_customer_orders
(
	order_id,
	customer_id,
	customer_key,
	order_status,
	order_date_key,
	order_delivered_customer_date_key
)
SELECT
	o.order_id,
	o.customer_id,
	dc.customer_key,
	o.order_status,

-- Use -1 as default key for missing or unknown dates

	COALESCE(d_purchase.date_key, -1) AS order_date_key,
	COALESCE(d_delivered.date_key, -1) AS order_delivered_customer_date_key
FROM silver.orders o

-- Attach customer surrogate key; LEFT JOIN preserves all orders

LEFT JOIN gold.dim_customers dc
ON dc.customer_id = o.customer_id

-- Map order purchase timestamp to date dimension

LEFT JOIN gold.dim_date d_purchase
ON d_purchase.full_date = CAST (o.order_purchase_timestamp AS DATE)

-- Map customer delivery date to date dimension

LEFT JOIN gold.dim_date d_delivered
ON d_delivered.full_date = CAST(o.order_delivered_customer_date AS DATE)

-- Purpose: Build order lifecycle fact with customer linkage, date keys, and delivery metrics
-- Grain: One row per order_id

INSERT INTO gold.fact_orders
(
	order_key,
	order_id,
	customer_key,
	customer_id,
	order_purchase_date_key,
	order_approved_date_key,
	order_delivered_carrier_date_key,
	order_delivered_customer_date_key,
	order_estimated_delivery_date_key,
	order_status,
	is_approval_date_null_invalid,
	is_delivered_carrier_date_null_invalid,
	is_delivered_customer_date_null_invalid,
	is_delivered_carrier_date_lessthan_approval_date,
	is_delivered_customer_date_lessthan_delivered_carrier_date,
	delivery_days,
	carrier_to_customer_days
)
SELECT 
	dco.order_key,
	o.order_id,
	dc.customer_key,
	o.customer_id,

-- Use -1 as default key for missing or unknown dates

	COALESCE(d_purchase.date_key, -1) AS order_purchase_date_key,
	COALESCE(d_approved.date_key, -1) AS order_approved_date_key,
	COALESCE(d_carrier.date_key, -1) AS order_delivered_carrier_date_key,
	COALESCE(d_delivered.date_key, -1) AS order_delivered_customer_date_key,
	COALESCE(d_estimated.date_key, -1) AS order_estimated_delivery_date_key,
	o.order_status,
	o.is_approval_date_null_invalid,
	o.is_delivered_carrier_date_null_invalid,
	o.is_delivered_customer_date_null_invalid,
	o.is_delivered_carrier_date_lessthan_approval_date,
	o.is_delivered_customer_date_lessthan_delivered_carrier_date,

-- Delivery duration metrics calculated in days

	DATEDIFF(DAY,o.order_purchase_timestamp, o.order_delivered_customer_date) AS delivery_days,
	DATEDIFF(DAY, o.order_delivered_carrier_date, o.order_delivered_customer_date ) AS carrier_to_customer_days
FROM silver.orders o

-- Attach order surrogate key; preserves all orders

LEFT JOIN gold.dim_customer_orders dco
ON dco.order_id = o.order_id

-- Attach customer surrogate key

LEFT JOIN gold.dim_customers dc
ON dc.customer_id = o.customer_id

-- Map order purchase timestamp to date dimension

LEFT JOIN gold.dim_date d_purchase
ON d_purchase.full_date = CAST (o.order_purchase_timestamp AS DATE)

-- Map order approval date to date dimension

LEFT JOIN gold.dim_date d_approved
ON d_approved.full_date = CAST(o.order_approved_at AS DATE)

-- Map order carrier handover date to date dimension

LEFT JOIN gold.dim_date d_carrier
ON d_carrier.full_date = CAST (o.order_delivered_carrier_date AS DATE)

-- Map order customer delivery date to date dimension

LEFT JOIN gold.dim_date d_delivered
ON d_delivered.full_date = CAST(o.order_delivered_customer_date AS DATE)

-- Map order estimated delivery date to date dimension

LEFT JOIN gold.dim_date d_estimated
ON d_estimated.full_date = CAST(o.order_estimated_delivery_date AS DATE);

-- Purpose: Build order item fact with customer, product, seller, and order context
-- Grain: One row per order_id and order_item_id

INSERT INTO gold.fact_sales
(
	order_key,
	order_id,
	order_item_id,
	customer_key,
	product_key,
	seller_key,
	order_date_key,
	shipping_limit_date_key,
	order_status,
	price,
	freight_value,
	gross_amount
)
SELECT
	dco.order_key,
	oi.order_id,
	oi.order_item_id,
	dc.customer_key,
	dp.product_key,
	ds.seller_key,

-- Use -1 as default key for missing or unknown dates

	COALESCE(d_purchase.date_key, -1) AS order_date_key,
	COALESCE(d_ship.date_key, -1) AS shipping_limit_date_key,
	dco.order_status,
	oi.price,
	oi.freight_value,

-- Gross amount includes item price plus freight value

	oi.price + oi.freight_value AS gross_amount
FROM silver.order_items oi

-- Attach order surrogate key and order status

LEFT JOIN gold.dim_customer_orders dco
ON dco.order_id = oi.order_id

-- Required join to retrieve customer and purchase date context

JOIN silver.orders o 
ON o.order_id = oi.order_id

-- Attach customer surrogate key

LEFT JOIN gold.dim_customers dc
ON dc.customer_id = o.customer_id

-- Attach product surrogate key

LEFT JOIN gold.dim_product dp
ON dp.product_id = oi.product_id

-- Attach seller surrogate key

LEFT JOIN gold.dim_sellers ds
ON ds.seller_id = oi.seller_id

-- Map order purchase date to date dimension

LEFT JOIN gold.dim_date d_purchase
ON d_purchase.full_date = CAST(o.order_purchase_timestamp AS DATE)

-- Map order shipping date to date dimension

LEFT JOIN gold.dim_date d_ship
ON d_ship.full_date = CAST(oi.shipping_limit_date AS DATE);

-- Purpose: Build order payment fact with customer context and purchase date
-- Grain: One row per order_id and payment_sequential

INSERT INTO gold.fact_payments
(
	order_id,
	payment_sequential,
	customer_key,
	order_date_key,
	payment_type,
	payment_installments,
	payment_value,
	is_zero_installments_with_value
)
SELECT
	p.order_id,
	p.payment_sequential,
	dc.customer_key,

-- Use -1 as default key for missing or unknown dates

	COALESCE(d_placed.date_key, -1) AS order_date_key,
	p.payment_type,
	p.payment_installments,
	p.payment_value,
	p.is_zero_installments_with_value
FROM silver.order_payments p

-- Required join to obtain customer and order purchase date context

JOIN silver.orders o
ON o.order_id = p.order_id

-- Attach customer surrogate key

LEFT JOIN gold.dim_customers dc
ON dc.customer_id = o.customer_id

-- Map order purchase date to date dimension

LEFT JOIN gold.dim_date d_placed 
ON d_placed.full_date = CAST(o.order_purchase_timestamp AS DATE);

-- Purpose: Build review fact by linking order reviews to customer, product, and seller context
-- Grain: One row per review_id

-- Select a single representative product and seller per order
-- using the first order item to avoid review duplication

;WITH ranked_items AS 
(
	SELECT
		order_id,
		product_id,
		seller_id,
		ROW_NUMBER() OVER(PARTITION BY order_id ORDER BY order_item_id) AS rn
	FROM silver.order_items
)
INSERT INTO gold.fact_reviews
(
	review_id,
	order_id,
	order_key,
	customer_key,
	product_key,
	seller_key,
	review_date_key,
	review_answer_date_key,
	review_score,
	has_review_comment
)
SELECT
	r.review_id,
	r.order_id,
	dco.order_key,

-- Use -1 as default key for missing or unknown dates

	COALESCE(dc.customer_key, -1) AS customer_key,
	COALESCE(dp.product_key, -1) AS product_key,
	COALESCE(ds.seller_key, -1) AS seller_key,
	COALESCE(d_review.date_key, -1) AS review_date_key,
	COALESCE(d_answer.date_key, -1) AS review_answer_date_key,
	r.review_score,

-- Flag reviews with non-empty comment text

	CASE WHEN NULLIF(TRIM(review_comment_message), '') IS NOT NULL THEN 1
		 ELSE 0
	END AS has_review_comment
FROM silver.order_reviews r

-- Attach order surrogate key

LEFT JOIN gold.dim_customer_orders dco
ON dco.order_id = r.order_id

-- Retrieve customer context for the reviewed order

LEFT JOIN silver.orders o
ON o.order_id = r.order_id

-- Attach representative product and seller for the reviewed order

LEFT JOIN ranked_items ri
ON ri.order_id = r.order_id AND ri.rn = 1

-- Attach customer surrogate key

LEFT JOIN gold.dim_customers dc
ON dc.customer_id = o.customer_id

-- Attach product surrogate key

LEFT JOIN gold.dim_product dp
ON dp.product_id = ri.product_id

-- Attach seller surrogate key

LEFT JOIN gold.dim_sellers ds
ON ds.seller_id = ri.seller_id

-- Map order review creation date to date dimension

LEFT JOIN gold.dim_date d_review
ON d_review.full_date = r.review_creation_date

-- Map review answer timestamp to date dimension

LEFT JOIN gold.dim_date d_answer
ON d_answer.full_date = CAST(r.review_answer_timestamp AS DATE);

-- Purpose: Build order delivery performance fact with lifecycle date keys and delivery metrics
-- Grain: One row per order_id

INSERT INTO gold.fact_shipping
(
	order_id,
	customer_key,
	order_key,
	order_purchase_date_key,
	order_approved_date_key,
	order_delivered_carrier_date_key,
	order_delivered_customer_date_key,
	order_estimated_delivery_date_key,
	days_to_approve,
	days_to_ship,
	days_in_transit,
	total_delivery_days,
	delivery_delay_days,
	delivery_status
)
SELECT
	o.order_id,
	dc.customer_key,
	dco.order_key,

-- Use -1 as default key for missing or unknown dates

	COALESCE(d_purchase.date_key, -1) AS order_purchase_date_key,
	COALESCE(d_approved.date_key, -1) AS order_approved_date_key,
	COALESCE(d_carrier.date_key, -1) AS order_delivered_carrier_date_key,
	COALESCE(d_customer.date_key, -1) AS order_delivered_customer_date_key,
	COALESCE(d_estimate.date_key, -1) AS order_estimated_delivery_date_key,

-- Delivery duration metrics calculated in days; NULLs propagate when dates are missing

	DATEDIFF(DAY, o.order_purchase_timestamp, o.order_approved_at )AS days_to_approve,
	DATEDIFF(DAY, CAST(o.order_approved_at AS DATE), CAST(o.order_delivered_carrier_date AS DATE)) AS days_to_ship,
	DATEDIFF(DAY, CAST(o.order_delivered_carrier_date AS DATE), CAST(o.order_delivered_customer_date AS DATE)) AS days_in_transit,
	DATEDIFF(DAY, CAST(o.order_purchase_timestamp AS DATE), CAST(o.order_delivered_customer_date AS DATE)) AS total_delivery_days,
	DATEDIFF(DAY, o.order_estimated_delivery_date, CAST(o.order_delivered_customer_date AS DATE)) AS delivery_delay_days,

-- Classify delivery outcome by comparing actual delivery date to estimated delivery date

	CASE WHEN CAST(o.order_estimated_delivery_date AS DATE) > CAST(o.order_delivered_customer_date AS DATE) THEN 'early'
		 WHEN CAST(o.order_estimated_delivery_date AS DATE) < CAST(o.order_delivered_customer_date AS DATE) THEN 'late'
		 WHEN o.order_estimated_delivery_date = CAST(o.order_delivered_customer_date AS DATE) THEN 'on time'
	END AS delivery_status
FROM silver.orders o

-- Attach order surrogate key

LEFT JOIN gold.dim_customer_orders dco
ON dco.order_id = o.order_id

-- Attach customer surrogate key

LEFT JOIN gold.dim_customers dc
ON dc.customer_id = o.customer_id

-- Map order purchase date to date dimension

LEFT JOIN gold.dim_date d_purchase
ON d_purchase.full_date = CAST(o.order_purchase_timestamp AS DATE)

-- Map order approval date to date dimension

LEFT JOIN gold.dim_date d_approved
ON d_approved.full_date = CAST(o.order_approved_at AS DATE)

-- Map carrier handover date to date dimension

LEFT JOIN gold.dim_date d_carrier
ON d_carrier.full_date = CAST(o.order_delivered_carrier_date AS DATE)

-- Map order delivered date to date dimension

LEFT JOIN gold.dim_date d_customer
ON d_customer.full_date = CAST(o.order_delivered_customer_date AS DATE)

-- Map order estimated delivery date to date dimension

LEFT JOIN gold.dim_date d_estimate
ON d_estimate.full_date = CAST(o.order_estimated_delivery_date AS DATE);

















