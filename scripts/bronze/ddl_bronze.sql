/*
====================================================================
DDL Script: Create Bronze Layer Tables
====================================================================

Script Purpose:
   This script creates tables in the 'bronze' schema, dropping existing tables
   if they already exist.
   Run this script to re-define the DDL structure of 'bronze' Tables

====================================================================
*/
use DataWarehouse;
GO

---------------------------------------------------bronze layer Creation------------------------------------------------------------------


---CRM Customer Info Table

if OBJECT_ID('bronze.crm_cust_info', 'u') IS NOT NULL
drop table bronze.crm_cust_info;
GO

CREATE TABLE bronze.crm_cust_info(
    cst_id INT,
    cst_key NVARCHAR(50),
    cst_first_name NVARCHAR(50),
    cst_last_name NVARCHAR(50),
    cst_material_status NVARCHAR(50),
    cst_gndr NVARCHAR(50),
    cast_created_date DATE,
);
GO


select * from bronze.crm_cust_info;

---CRM Product Info Table

if OBJECT_ID('bronze.crm_prd_info', 'u') IS NOT NULL
drop table bronze.crm_prd_info;
GO

CREATE TABLE bronze.crm_prd_info(
    prd_id INT,
    prd_key NVARCHAR(50),
    prd_nm NVARCHAR(50),
    prd_cost NVARCHAR(50),
    prd_line NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt DATETIME
);
GO

select * from bronze.crm_prd_info;

---CRM Sales Details Table

if OBJECT_ID('bronze.crm_sales_details', 'u') IS NOT NULL
drop table bronze.crm_sales_details;
GO

create table bronze.crm_sales_details(
    sls_ord_num NVARCHAR(50),
    sls_prd_key NVARCHAR(50),
    sls_cust_id INT,
    sls_order_dt int,
    sls_ship_dt int,
    sls_due_dt int,
    sls_sales int,
    sls_quantity int,
    sls_price int,
);
GO

select * from bronze.crm_sales_details;


--erp location_a101 table

if OBJECT_ID('bronze.erp_loc_a101', 'u') IS NOT NULL
drop table bronze.erp_loc_a101; 
GO

create table bronze.erp_loc_a101(
    cid NVARCHAR(50),
   cntry nvarchar(50),
    
);
GO

select * from bronze.erp_loc_a101;


--erp customer_az12 table

if OBJECT_ID('bronze.erp_cust_az12', 'u') IS NOT NULL
drop table bronze.erp_cust_az12;
GO

create table bronze.erp_cust_az12(
    cid NVARCHAR(50),
    bdate DATE,
    gen NVARCHAR(50)
);
GO

select * from bronze.erp_cust_az12;

--ERP px_cat_g1v2 table

if OBJECT_ID('bronze.erp_px_cat_g1v2', 'u') IS NOT NULL
drop table bronze.erp_px_cat_g1v2; 
GO

create table bronze.erp_px_cat_g1v2(
    id NVARCHAR(50),
    cat NVARCHAR(50),
    subcat NVARCHAR(50),
    maintenance NVARCHAR(50)
);
GO

select * from bronze.erp_px_cat_g1v2;
GO



