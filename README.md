# E-Commerce Data Warehouse & Analytics Project

## Project Overview

This project demonstrates an end-to-end SQL-based ecommerce data warehouse and Power BI analytics solution designed for business reporting and decision-making.
The solution follows a layered analytics architecture (Bronze–Silver–Gold) and includes:
- Data ingestion and transformation using SQL
- Analytics-ready fact and dimension tables
- Interactive Power BI dashboards for executive and operational insights
This project was built as part of my hands-on upskilling in SQL, data warehousing, and Power BI.

## Architecture Overview

The project follows a Medallion (Bronze–Silver–Gold) architecture to ensure scalability, data quality, and clear separation of concerns.
- Bronze layer: Raw data ingestion
- Silver layer: Data cleaning and standardization
- Gold layer: Business-ready fact and dimension tables used for analytics.

## Data Model

The analytics layer is designed using a star schema to support efficient reporting and DAX calculations in Power BI.
- Fact tables store transactional and measurable data
- Dimension tables provide descriptive context
- Surrogate keys are used for relationships

## Power BI Dashboards

The Power BI report is built on top of the Gold layer tables and provides insights across multiple business areas.
1. Executive Overview
2. Delivery & Operations
3. Customer & Product Insights
4. Customer Feedback & Reviews

## Key Metrics
- Total Revenue
- Average Order Value
- On-Time Delivery %
- Customer Satisfaction Metrics

## Power BI Report File

The complete Power BI report file is included in this repository:
📄 `powerbi/ecommerce_analytics_dashboard.pbix`
This file contains:
- Star schema data model (facts & dimensions)
- DAX measures for revenue, delivery, and customer insights
## Architecture
- Bronze Layer: Raw ingested data
- Silver Layer: Cleaned & transformed data
- Gold Layer: Analytics-ready fact and dimension tables

## Technologies Used
- SQL Server: Data modeling, transformations, CTEs, window functions
- Power BI: DAX, data modeling, dashboard

## Key Skills Demonstrated

- Data warehousing concepts (Medallion architecture)
- Star schema data modeling
- Business-focused DAX measures
- Power BI dashboard design
- Structured project documentation




- Interactive dashboards used for business reporting

