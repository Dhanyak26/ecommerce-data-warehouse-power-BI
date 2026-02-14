## DAX Measures

This file documents key DAX measures used in the Power BI report.

1.	Total revenue(delivered)
DAX
    Total revenue(delivered) = CALCULATE([Total revenue], 'gold fact_sales'[order_status] = "delivered")
Purpose: 
    Calculates the total revenue from only delivered orders.
Business Context:
     Used in the executive overview dashboard to track actual revenue that contributes to business performance.

2.	Average order value(delivered)
DAX
    Average order value(delivered) = DIVIDE([Total revenue(delivered)],[Total orders])
Purpose: 
    Computes the average revenue per order for delivered orders.
Business Context: 
    Helps executives understand average customer spending and assess revenue trends.

3.  On time delivery %
DAX
    On time delivery % = DIVIDE([On time delivered orders],[Delivered orders])
Purpose: 
    Calculates the percentage of orders delivered on time.
Business Context: 
    Key operational KPI used in the delivery & operations dashboard.

4.	Early delivery %
DAX 
    Early delivery % = DIVIDE([Early delivered orders],[Delivered orders])
Purpose: 
    Measures the proportion of orders delivered earlier than expected.
Business Context: 
    Helps operations team evaluate efficiency and logistics performance.

5.	High rating % (4-5)
DAX
    High rating % (4-5) = DIVIDE([High rating orders],[Total reviews])
Purpose: 
    Computes percentage of customer reviews with high ratings (4 or 5).
Business Context: 
    Shows customer satisfaction trends in the feedback & reviews dashboard.

6.	Average review score
DAX
    Average review score = AVERAGE('gold fact_reviews'[review_score])
Purpose:
     Calculates the mean review score from all customer feedback.
Business Context: 
    Tracks overall product/service satisfaction and identifies trends over time.

7.	Average orders per customer 
DAX
    Average orders per customer = DIVIDE(DISTINCTCOUNT('gold fact_orders'[order_id]), DISTINCTCOUNT('gold fact_orders'[customer_id]))
Purpose:
     Measures the average number of orders placed per customer.
Business Context:
     Provides insights into repeat purchase behavior and customer engagement.
