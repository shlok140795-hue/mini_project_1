#Section B - SQL Engineering 
#Q1. Intermediate SQL Queries 
CREATE DATABASE mini_project1;
USE mini_project1;
SHOW TABLES;
#Write SQL queries to: 
#1. Find top 10 customers by revenue. 
DESCRIBE customers;
DESCRIBE orders;
SELECT
    c.customer_id,
    c.customer_name,
    SUM(o.quantity * o.unit_price) AS total_revenue
FROM customers c
JOIN orders o
    ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.customer_name
ORDER BY total_revenue DESC
LIMIT 10;
2. Find month-over-month sales growth. 
WITH monthly_sales AS (
    SELECT
        DATE_FORMAT(order_date, '%Y-%m') AS sales_month,
        SUM(quantity * unit_price) AS total_sales
    FROM orders
    GROUP BY DATE_FORMAT(order_date, '%Y-%m')
),
sales_with_previous AS (
    SELECT
        sales_month,
        total_sales,
        LAG(total_sales) OVER (
            ORDER BY sales_month
        ) AS previous_month_sales
    FROM monthly_sales
)
SELECT
    sales_month,
    total_sales,
    previous_month_sales,
    ROUND(
        (total_sales - previous_month_sales)
        / NULLIF(previous_month_sales, 0) * 100,
        2
    ) AS mom_growth_percentage
FROM sales_with_previous
ORDER BY sales_month;
3. Find customers who ordered in consecutive months.  
WITH customer_months AS (
    SELECT DISTINCT
        customer_id,
        DATE_FORMAT(order_date, '%Y-%m') AS order_month
    FROM orders
),
previous_months AS (
    SELECT
        customer_id,
        order_month,
        LAG(order_month) OVER (
            PARTITION BY customer_id
            ORDER BY order_month
        ) AS previous_month
    FROM customer_months
)
SELECT
    customer_id,
    previous_month,
    order_month
FROM previous_months
WHERE STR_TO_DATE(CONCAT(order_month, '-01'), '%Y-%m-%d')
    = DATE_ADD(
        STR_TO_DATE(CONCAT(previous_month, '-01'), '%Y-%m-%d'),
        INTERVAL 1 MONTH
    );
4. Find products never ordered. 
SELECT
    p.product_id,
    p.product_name
FROM products p
LEFT JOIN orders o
    ON p.product_id = o.product_id
WHERE o.product_id IS NULL; 
#just to verify distinct count of product
SELECT 
    COUNT(DISTINCT product_id)
FROM orders;
5. Find revenue contribution percentage by category.  
SELECT
    p.category,
    ROUND(SUM(o.quantity * o.unit_price) * 100.0 /
        (SELECT SUM(quantity * unit_price)
         FROM orders),
        2) AS revenue_contribution_percentage
FROM products p
LEFT JOIN orders o
    ON p.product_id = o.product_id
GROUP BY p.category

#Q2. Advanced SQL  
#Tasks 
#1. Rank customers based on total revenue.
  WITH revenue AS (SELECT
    o.customer_id, c.customer_name,
    SUM(quantity*unit_price) AS revenue
    FROM orders o
    JOIN customers c
    ON c.customer_id =o.customer_id
    GROUP BY o.customer_id,c.customer_name)
  SELECT customer_id, customer_name, revenue,
    ROW_NUMBER() OVER (ORDER BY revenue DESC) AS row_num,
    RANK() OVER (ORDER BY revenue DESC) AS rnk,
    DENSE_RANK() OVER (ORDER BY revenue DESC) AS dense_rnk
  FROM revenue;
  
2. Find running total sales by month.
WITH monthly_sales AS (
    SELECT
        DATE_FORMAT(order_date, '%Y-%m') AS sales_month,
        SUM(quantity * unit_price) AS monthly_revenue
    FROM orders
    GROUP BY DATE_FORMAT(order_date, '%Y-%m')
)
SELECT
    sales_month,
    monthly_revenue,
    SUM(monthly_revenue) OVER (
        ORDER BY sales_month
    ) AS running_total
FROM monthly_sales
ORDER BY sales_month;
3. Find the highest selling product per category. 
WITH product_sales AS (
    SELECT
        p.product_id,
        p.product_name,
        p.category,
        SUM(o.quantity) AS total_quantity_sold
    FROM products p
    JOIN orders o
        ON p.product_id = o.product_id
    GROUP BY
        p.product_id,
        p.product_name,
        p.category
),
ranked_products AS (
    SELECT
        *,
        DENSE_RANK() OVER (
            PARTITION BY category
            ORDER BY total_quantity_sold DESC
        ) AS product_rank
    FROM product_sales
)
SELECT
    category,
    product_id,
    product_name,
    total_quantity_sold
FROM ranked_products
WHERE product_rank = 1; 

#OR

SELECT
    category,
    product_id,
    product_name,
    total_quantity_sold
FROM (
    SELECT
        p.category,
        p.product_id,
        p.product_name,
        SUM(o.quantity) AS total_quantity_sold,
        DENSE_RANK() OVER (
            PARTITION BY p.category
            ORDER BY SUM(o.quantity) DESC
        ) AS rnk
    FROM products p
    JOIN orders o
        ON p.product_id = o.product_id
    GROUP BY
        p.category,
        p.product_id,
        p.product_name
) AS ranked_products
WHERE rnk = 1;
    
4. Find 7-day rolling average sales. 
SELECT
    sales_date,
    total_sales,
    ROUND(AVG(total_sales) OVER (
        ORDER BY sales_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ),2) AS rolling_avg
FROM (
    SELECT
        DATE(order_date) AS sales_date,
        SUM(quantity * unit_price) AS total_sales
    FROM orders
    GROUP BY DATE(order_date)
) AS daily_sales;

#using CTE
WITH daily_sales AS (
    SELECT
        DATE(order_date) AS sales_date,
        SUM(quantity * unit_price) AS total_sales
    FROM orders
    GROUP BY DATE(order_date)
)
SELECT
    sales_date,
    total_sales,
    ROUND(AVG(total_sales) OVER (
        ORDER BY sales_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ),2) AS rolling_avg
FROM daily_sales;

#Q3.
#EXPLAIN ANALYZE
ALTER TABLE customers
ADD PRIMARY KEY (customer_id)
DESCRIBE orders;
CREATE INDEX idx_orders_order_date
ON orders(order_date(10));
SELECT
    c.customer_name,
    SUM(o.quantity * o.unit_price) AS total_revenue
FROM orders o
JOIN customers c
    ON o.customer_id = c.customer_id
WHERE o.order_date >= '2025-01-01'
GROUP BY c.customer_name;