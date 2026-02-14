/*
File Name      : bronze_load.sql
Layer          : Bronze
Purpose        : Load raw data into bronze tables.
Description    :
- Truncate existing bronze tables.
- Inserts data from source files into bronze-layer tables using BULK INSERT.
- Preserves original data structure and values.
- Acts as the ingestion step of the pipeline.

Notes          :
- Assumes source files are already available.
- No deduplication or cleansing is performed here.
- Should be executed after bronze table creation.
- TRUNCATE is intentional to ensure full reload.
- Source file paths are environment-specific.
- Designed for initial ingestion and reprocessing.
*/

-- Assumption: source CSV files are complete extracts (full load)

-- ============================================
-- Load customer master data
-- ============================================

TRUNCATE TABLE bronze.customer_info;

BULK INSERT bronze.customer_info

-- Note: file paths should be parameterized or externalized in production


FROM 'C:\Users\91779\Desktop\Dhanya\sql\dataset\olist dataset\olist_customers_dataset.csv'
WITH
(
	FORMAT = 'CSV',
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	FIELDQUOTE = '"',
	ROWTERMINATOR = '0x0a',
	CODEPAGE = '65001',
	TABLOCK
);

-- ============================================
-- Load geolocation master data
-- ============================================

TRUNCATE TABLE bronze.geolocation;

BULK INSERT bronze.geolocation
FROM 'C:\Users\91779\Desktop\Dhanya\sql\dataset\olist dataset\olist_geolocation_dataset.csv'
WITH
(
	FORMAT = 'CSV',
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	FIELDQUOTE = '"',
	ROWTERMINATOR = '0x0a',
	CODEPAGE = '65001',
	TABLOCK
);

-- ============================================
-- Load order items master data
-- ============================================

TRUNCATE TABLE bronze.order_items;

BULK INSERT bronze.order_items
FROM 'C:\Users\91779\Desktop\Dhanya\sql\dataset\olist dataset\olist_order_items_dataset.csv'
WITH
(
	FORMAT = 'CSV',
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	FIELDQUOTE = '"',
	ROWTERMINATOR = '0x0a',
	CODEPAGE = '65001',
	TABLOCK
);

-- ============================================
-- Load order payments master data
-- ============================================

TRUNCATE TABLE bronze.order_payments;

BULK INSERT bronze.order_payments
FROM 'C:\Users\91779\Desktop\Dhanya\sql\dataset\olist dataset\olist_order_payments_dataset.csv'
WITH
(
	FORMAT = 'CSV',
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	FIELDQUOTE = '"',
	ROWTERMINATOR = '0x0a',
	CODEPAGE = '65001',
	TABLOCK
);

-- ============================================
-- Load order reviews master data
-- ============================================

TRUNCATE TABLE bronze.order_reviews;

BULK INSERT bronze.order_reviews
FROM 'C:\Users\91779\Desktop\Dhanya\sql\dataset\olist dataset\olist_order_reviews_dataset.csv'
WITH
(
	FORMAT = 'CSV',
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	FIELDQUOTE = '"',
	ROWTERMINATOR = '0x0d0a',
	CODEPAGE = '65001',
	TABLOCK
);

-- ============================================
-- Load orders master data
-- ============================================

TRUNCATE TABLE bronze.orders;

BULK INSERT bronze.orders
FROM 'C:\Users\91779\Desktop\Dhanya\sql\dataset\olist dataset\olist_orders_dataset.csv'
WITH
(
	FORMAT = 'CSV',
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	FIELDQUOTE = '"',
	ROWTERMINATOR = '0x0a',
	CODEPAGE = '65001',
	TABLOCK
);

-- ============================================
-- Load products master data
-- ============================================

TRUNCATE TABLE bronze.products;

BULK INSERT bronze.products
FROM 'C:\Users\91779\Desktop\Dhanya\sql\dataset\olist dataset\olist_products_dataset.csv'
WITH
(
	FORMAT = 'CSV',
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	FIELDQUOTE = '"',
	ROWTERMINATOR = '0x0a',
	CODEPAGE = '65001',
	TABLOCK
);

-- ============================================
-- Load sellers master data
-- ============================================

TRUNCATE TABLE bronze.sellers;

BULK INSERT bronze.sellers
FROM 'C:\Users\91779\Desktop\Dhanya\sql\dataset\olist dataset\olist_sellers_dataset.csv'
WITH
(
	FORMAT = 'CSV',
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	FIELDQUOTE = '"',
	ROWTERMINATOR = '0x0a',
	CODEPAGE = '65001',
	TABLOCK
);

-- ======================================================
-- Load product category name translation master data
-- ======================================================

TRUNCATE TABLE bronze.product_category_name_translation;

BULK INSERT bronze.product_category_name_translation
FROM 'C:\Users\91779\Desktop\Dhanya\sql\dataset\olist dataset\product_category_name_translation.csv'
WITH
(
	FORMAT = 'CSV',
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	ROWTERMINATOR = '0x0a',
	CODEPAGE = '65001',
	TABLOCK
);