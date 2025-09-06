
/*
====================================================================
DDL Script: Create silver Layer Tables
====================================================================

Script Purpose:
   This script creates tables in the 'silver' schema, dropping existing tables
   if they already exist.
   Run this script to re-define the DDL structure of 'silver' Tables

====================================================================
*/
use DataWarehouse;
GO

---------------------------------------------------silver layer Creation------------------------------------------------------------------


---CRM Customer Info Table

if OBJECT_ID('silver.crm_cust_info', 'u') IS NOT NULL
drop table silver.crm_cust_info;
GO

CREATE TABLE silver.crm_cust_info(
    cst_id INT,
    cst_key NVARCHAR(50),
    cst_first_name NVARCHAR(50),
    cst_last_name NVARCHAR(50),
    cst_marital_status NVARCHAR(50),
    cst_gndr NVARCHAR(50),
    cast_created_date DATE,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
    

);
GO


select * from silver.crm_cust_info;

---CRM Product Info Table

if OBJECT_ID('silver.crm_prd_info', 'u') IS NOT NULL
drop table silver.crm_prd_info;
GO

CREATE TABLE silver.crm_prd_info(
    prd_id INT,
    cat_id NVARCHAR(50),
    prd_key NVARCHAR(50),
    prd_nm NVARCHAR(50),
    prd_cost INT,
    prd_line NVARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt DATE,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

select * from silver.crm_prd_info;

---CRM Sales Details Table

if OBJECT_ID('silver.crm_sales_details', 'u') IS NOT NULL
drop table silver.crm_sales_details;
GO

create table silver.crm_sales_details(
    sls_ord_num NVARCHAR(50),
    sls_prd_key NVARCHAR(50),
    sls_cust_id INT,
    sls_order_dt DATE,
    sls_ship_dt DATE,
    sls_due_dt DATE,
    sls_sales int,
    sls_quantity int,
    sls_price int,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

select * from silver.crm_sales_details;


--erp location_a101 table

if OBJECT_ID('silver.erp_loc_a101', 'u') IS NOT NULL
drop table silver.erp_loc_a101; 
GO

create table silver.erp_loc_a101(
    cid NVARCHAR(50),
    cntry nvarchar(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
    
);
GO

select * from silver.erp_loc_a101;


--erp customer_az12 table

if OBJECT_ID('silver.erp_cust_az12', 'u') IS NOT NULL
drop table silver.erp_cust_az12;
GO

create table silver.erp_cust_az12(
    cid NVARCHAR(50),
    bdate DATE,
    gen NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

select * from silver.erp_cust_az12;

--ERP px_cat_g1v2 table

if OBJECT_ID('silver.erp_px_cat_g1v2', 'u') IS NOT NULL
drop table silver.erp_px_cat_g1v2; 
GO

create table silver.erp_px_cat_g1v2(
    id NVARCHAR(50),
    cat NVARCHAR(50),
    subcat NVARCHAR(50),
    maintenance NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

select * from silver.erp_px_cat_g1v2;
GO