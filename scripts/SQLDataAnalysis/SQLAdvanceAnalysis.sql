
--------------------------------------Change Over Time(Trends) Analysis--------------------------------------------------------------------

SELECT
YEAR(order_date) as year,
MONTH(order_date) as month,
SUM(sales_amount) as total_revenue,
COUNT(distinct customer_key) as total_customers,
SUM(quantity) as total_quantity
from gold.fact_sales
WHERE order_date is not null
GROUP BY YEAR(order_date),MONTH(order_date)
ORDER BY YEAR(order_date),MONTH(order_date)


--or

SELECT
DATETRUNC(YEAR,order_date) as YEAR,
SUM(sales_amount) as total_revenue,
COUNT(distinct customer_key) as total_customers,
SUM(quantity) as total_quantity
from gold.fact_sales
WHERE order_date is not null
GROUP BY DATETRUNC(YEAR,order_date) 
ORDER BY DATETRUNC(YEAR,order_date)

--or

SELECT
FORMAT(order_date, 'yyyy-MM') as order_date,
SUM(sales_amount) as total_revenue,
COUNT(distinct customer_key) as total_customers,
SUM(quantity) as total_quantity
from gold.fact_sales
WHERE order_date is not null
GROUP BY FORMAT(order_date, 'yyyy-MM') 
ORDER BY FORMAT(order_date, 'yyyy-MM')


-------------------------------------------------- Cumulative Analysis-----------------------------------------------------------------------


-- Calculate the total sales per month the running total  of sales and and moving average of price over time

SELECT 
order_date,
total_sales,
SUM(total_sales) OVER(ORDER BY order_date) as running_total,
AVG(average_price) OVER(ORDER BY order_date) as moving_average_price
from (
SELECT 
DATETRUNC(MONTH,order_date) as order_date,
SUM(sales_amount) as total_sales,
AVG(price) as average_price
from gold.fact_sales
WHERE order_date is not null
GROUP BY DATETRUNC(MONTH,order_date)
) t

--or 

SELECT 
order_date,
total_sales,
SUM(total_sales) OVER(ORDER BY order_date) as running_total,
AVG(average_price) OVER(ORDER BY order_date) as moving_average_price
from (
SELECT 
DATETRUNC(YEAR,order_date) as order_date,
SUM(sales_amount) as total_sales,
AVG(price) as average_price
from gold.fact_sales
WHERE order_date is not null
GROUP BY DATETRUNC(YEAR,order_date)
) t



---------------------------------------------------- Performance Analysis-------------------------------------------------------------------



/* Analyze the yearly performance of products by comparing their sales
to both the average sales performance of the product and the previous year's sales */
with yearly_product_sales as (
SELECT
YEAR(order_date) as order_year,
p.product_name,
SUM(sales_amount) as current_sales
from gold.fact_sales f
LEFT JOIN gold.dim_products p
on f.product_key = p.product_key
WHERE order_date is not null
GROUP BY YEAR(order_date),p.product_name
)
SELECT
order_year,
product_name,
current_sales,
AVG(current_sales) OVER(PARTITION BY product_name) as avg_sales,
current_sales - AVG(current_sales) OVER(PARTITION BY product_name) as diff_avg,
case 
    when current_sales - AVG(current_sales) OVER(PARTITION BY product_name) > 0  then 'Above Avg' 
    when current_sales - AVG(current_sales) OVER(PARTITION BY product_name) < 0  then 'Below Avg'
    else 'Avg'
    end as avg_change,
    --Year-over-Year analysis
    LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year) as py_sales,
    current_sales - LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year) as diff_py,
    case 
    when current_sales - LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year) > 0  then 'Increase'
    when current_sales - LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year) < 0  then 'Decrease'
    else 'No Change'
    end as py_change
from yearly_product_sales
ORDER BY product_name,order_year




---------------------------------------------- Part-To-Whole Analysis---------------------------------------------------------------------


--- Which categories contribute the most to overall sales?

with category_sales as (
SELECT
p.category,
SUM(sales_amount) as total_sales
from gold.fact_sales f
LEFT JOIN gold.dim_products p
on f.product_key = p.product_key
GROUP BY p.category
)
select
category,
total_sales,
SUM(total_sales) OVER() as overall_sales,
concat(round(CAST(total_sales as float) / SUM(total_sales) OVER() * 100,2),'%') as percentage
from category_sales
ORDER BY total_sales DESC


----------------------------------------------- Data Segmentation-------------------------------------------------------------------------

/*Segment products into cost ranges and count how many products fall into each segment*/

with product_segment as (
    SELECT
    product_key,
    product_name,
    cost,
    CASE
        WHEN cost < 100 THEN 'Below 100'
        WHEN cost BETWEEN 100 AND 500 THEN '100-500'
        WHEN cost BETWEEN 500 AND 1000 THEN '500-1000'
        ELSE 'Above 1000'
    END as cost_range
    from gold.dim_products
)
SELECT
cost_range,
COUNT(product_key) as total_products
from product_segment
GROUP BY cost_range
ORDER BY total_products DESC



/*Group customers into three segments based on their spending behavior:
    - VIP: Customers with at least 12 months of history and spending more than €5,000.
    - Regular: Customers with at least 12 months of history but spending €5,000 or less.
    - New: Customers with a lifespan less than 12 months.
And find the total number of customers by each group
*/

WITH customer_spending as (
SELECT
c.customer_key,
SUM(f.sales_amount) as total_spending,
MIN(order_date) as first_order,
MAX(order_date) as last_order,
DATEDIFF(MONTH,MIN(order_date),MAX(order_date)) as lifespan
from gold.fact_sales f
LEFT JOIN gold.dim_customers c
on f.customer_key = c.customer_key  
GROUP BY c.customer_key
)
SELECT
customer_segment,
COUNT(customer_key) as total_customers
from (
select 
customer_key,
total_spending,
lifespan,
case
    when lifespan >= 12 and total_spending > 5000 then 'VIP'
    when lifespan >= 12 and total_spending <= 5000 then 'Regular'
    else 'New'
end as customer_segment
from customer_spending
) t
GROUP BY customer_segment
ORDER BY total_customers DESC



--------------------------------------------------- Customer Report -------------------------------------------------------------------------

/*
===========================================================
Customer Report
===========================================================

Purpose:
 - This report consolidates key customer metrics and behaviors

Highlights:
 1. Gathers essential fields such as names, ages, and transaction details.
 2. Segments customers into categories (VIP, Regular, New) and age groups.
 3. Aggregates customer-level metrics:
    - total orders
    - total sales
    - total quantity purchased
    - total products
    - lifespan (in months)
 4. Calculates valuable KPIs:
    - recency (months since last order)
    - average order value
    - average monthly spend
===========================================================
*/

GO
create or alter view gold.report_customers as
WITH base_query as (
/*---------------------------------------------------------
1) Base Query: Retrieves core columns from tables
---------------------------------------------------------*/
SELECT 
f.order_number,
f.product_key,
f.order_date,
f.sales_amount,
f.quantity,
c.customer_key,
c.customer_number,
CONCAT(c.first_name,' ',c.last_name) as customer_name,
DATEDIFF(YEAR,c.birthdates,GETDATE()) as age
from gold.fact_sales f
LEFT JOIN gold.dim_customers c
on f.customer_key = c.customer_key
WHERE order_date is NOT NULL
),
customer_aggregation as (

/*--------------------------------------------------------------------------
2) Customer Aggregations: Summarizes key metrics at the customer level
--------------------------------------------------------------------------*/

    SELECT 
    customer_key,
    customer_number,
    customer_name,
    age,
    COUNT(distinct order_number) as total_orders,
    SUM(sales_amount) as total_sales,
    SUM(quantity) as total_quantity,
    COUNT(distinct product_key) as total_products,
    MAX(order_date) as last_order_date,
    DATEDIFF(MONTH,MIN(order_date),MAX(order_date)) as lifespan
    from base_query
    GROUP BY customer_key,customer_number,customer_name,age
)
SELECT
customer_key,
customer_number,
customer_name,
age,
case
    when age < 20 then 'Under 20'
    when age between 20 and 29 then '20-29'
    when age between 30 and 39 then '30-39'
    when age between 40 and 49 then '40-49'
    else '50 and above'
end as age_group,
case
    when lifespan >= 12 and total_sales > 5000 then 'VIP'
    when lifespan >= 12 and total_sales <= 5000 then 'Regular'
    else 'New'
end as customer_segment,
last_order_date,
DATEDIFF(MONTH,last_order_date,GETDATE()) as recency,
total_orders,
total_sales,
total_quantity,
total_products,
lifespan,
--compute average order value (AVO)
case 
    when total_orders = 0 then 0
    else total_sales/total_orders
end as avg_order_value,
case
    when lifespan = 0 then total_sales
    else total_sales/lifespan
end as avg_monthly_spend
from customer_aggregation
GO

SELECT * from gold.report_customers;




/*
====================================
Product Report
====================================
Purpose:
- This report consolidates key product metrics and behaviors.

Highlights:
1. Gathers essential fields such as product name, category, subcategory, and cost.
2. Segments products by revenue to identify High-Performers, Mid-Range, or Low-Performers.
3. Aggregates product-level metrics:
- total orders
- total sales
- total quantity sold
- total customers (unique)
- lifespan (in months)
4. Calculates valuable KPIs:
- recency (months since last sale)
- average order revenue (AOR)
- average monthly revenue

*/

GO
--SELECT top 100 * from gold.report_products
GO

CREATE or ALTER view gold.report_products as
with base_query as (
/*----------------------------------------------------------------------------
1) Base Query: Retrieves core columns from fact_sales and dim_products tables
------------------------------------------------------------------------------*/
SELECT 
f.order_number,
f.order_date,
f.customer_key,
f.sales_amount,
f.quantity,
p.product_key,
p.product_name,
p.category,
p.sub_category,
p.cost
from gold.fact_sales f
LEFT JOIN gold.dim_products p
on f.product_key = p.product_key
WHERE order_date is NOT NULL
),
product_aggregation as (
/*----------------------------------------------------------------------------
2) Product Aggregations: Summarizes key metrics at the product level
------------------------------------------------------------------------------*/
    SELECT 
    product_key,
    product_name,
    category,
    sub_category,
    cost,
    DATEDIFF(MONTH,MIN(order_date),MAX(order_date)) as lifespan,
    MAX(order_date) as last_sale_date,
    COUNT(distinct order_number) as total_orders,
    COUNT(distinct customer_key) as total_customers,
    SUM(sales_amount) as total_sales,
    SUM(quantity) as total_quantity,
    round(CAST(SUM(sales_amount) as float)/nullif(SUM(quantity),0),2) as avg_selling_price
    from base_query
    GROUP BY product_key,product_name,category,sub_category,cost
)
/*----------------------------------------------------------
3) Final Query: Combine all product results into one output 
------------------------------------------------------------*/
SELECT
product_key,
product_name,
category,
sub_category,
cost,
last_sale_date,
DATEDIFF(MONTH,last_sale_date,GETDATE()) as recency_in_months,
case
    when total_sales >= 50000 then 'High Performer'
    when total_sales >= 10000 then 'Mid Range'
    else 'Low Performer'
end as product_segment,
lifespan,
total_orders,
total_sales,
total_quantity,
total_customers,
avg_selling_price,
--compute average order revenue (AOR)
case 
    when total_orders = 0 then 0
    else total_sales/total_orders
end as avg_order_revenue,
--Average Monthly Revenue
case
    when lifespan = 0 then total_sales
    else total_sales/lifespan
end as avg_monthly_revenue
from product_aggregation
GO


