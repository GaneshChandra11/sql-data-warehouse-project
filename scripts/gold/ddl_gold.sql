use DataWarehouse;
GO


/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

---------------------------------------------------gold layer Creation------------------------------------------------------------------

---data validation of ci.cst_gndr and ca.gen

SELECT
    ci.cst_gndr,
    ca.gen,
    case when ci.cst_gndr != 'n/a' then ci.cst_gndr
        else coalesce(ca.gen,'n/a')
    end as new_gen
from silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca 
on        ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
on        ci.cst_key = la.cid
GO

-----------------------------------------------silver.crm_cust_info tbl view creation in gold layer------------------------------


IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
drop view gold.dim_customers;
GO

CREATE OR ALTER VIEW gold.dim_customers as
SELECT
    ROW_NUMBER() OVER (ORDER BY cst_id) as customer_key,
    ci.cst_id as customer_id,
    ci.cst_key as customer_number,
    ci.cst_first_name as first_name,
    ci.cst_last_name as last_name,
    la.cntry as country,
    ci.cst_marital_status as marital_status,
    case when ci.cst_gndr != 'n/a' then ci.cst_gndr 
        else coalesce(ca.gen,'n/a')
    end as gender,                                  -- CRM is the Master for gender Info
    ca.bdate as birthdates,
    ci.cst_created_date as create_date
from silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca 
on        ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
on        ci.cst_key = la.cid
GO


select *  from silver.crm_cust_info

------------------------------------check the quality of gold.dim_customers(View)------------------------------------------------------------

SELECT * from gold.dim_customers

SELECT distinct gender
from gold.dim_customers
GO


-----------------------------------------------silver.crm_prd_info tbl view creation in gold layer------------------------------------

IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
drop view gold.dim_products;
GO

CREATE or ALTER VIEW gold.dim_products as
SELECT
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) as product_key, 
    pn.prd_id as product_id,
    pn.prd_key as product_number,
    pn.prd_nm as product_name,
    pn.cat_id as category_id,
    pc.cat as category,
    pc.subcat as sub_category,
    pc.maintenance,
    pn.prd_cost as cost,
    pn.prd_line as product_line,
    pn.prd_start_dt as start_date
from silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
on pn.cat_id = pc.id
where prd_end_dt is null 
GO



----------------------------------------------check the quality of gold.dim_products(View)------------------------------------------------

select prd_key, COUNT(*) FROM (
SELECT 
    pn.prd_id,
    pn.prd_key,
    pn.prd_nm,
    pn.cat_id,
    pc.cat,
    pc.subcat,
    pc.maintenance,
    pn.prd_cost,
    pn.prd_line,
    pn.prd_start_dt
from silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
on pn.cat_id = pc.id
where prd_end_dt is null                           --Filter out all Historical data (current products only)
)t 
GROUP BY prd_key
HAVING COUNT(*) > 1
GO




---------------------------------silver.crm_sales_details tbl view creation in gold layer------------------------------------------


IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
drop view gold.fact_sales;
GO

CREATE or ALTER VIEW gold.fact_sales as
SELECT 
    sd.sls_ord_num as order_number,
    pr.product_key,
    cu.customer_key,
    sd.sls_order_dt as order_date,
    sd.sls_ship_dt as shipping_date,
    sd.sls_due_dt as due_date,
    sd.sls_sales as sales_amount,
    sd.sls_quantity as quantity,
    sd.sls_price as price
from silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
on sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
on sd.sls_cust_id = cu.customer_id
GO


----------------------------------------------check the quality of gold.fact_sales(View)------------------------------------------------


SELECT * from gold.fact_sales f
LEFT JOIN gold.dim_customers c
on f.customer_key = c.customer_key 
LEFT JOIN gold.dim_products p
on f.product_key = p.product_key
WHERE p.product_key is null; 


