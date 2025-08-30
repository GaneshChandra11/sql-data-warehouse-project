/*
=============================================================
Create Database and Schemas
=============================================================

Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking if it already exists.
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas
    within the database: 'bronze', 'silver', and 'gold'.

WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists.
    All data in the database will be permanently deleted. Proceed with caution
    and ensure you have proper backups before running this script.
*/




use master;
GO


--drop and recreate Data Warehouse Database if it exists
IF EXISTS (SELECT * FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END 



--Create Data Warehouse Database
CREATE DATABASE DataWarehouse;
go


use DataWarehouse;
GO


-----Create Bronze, Silver and Gold Schemas

/*create schema bronze;
go
create schema silver;
go
create schema gold;
go
*/

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'bronze')
    EXEC('CREATE SCHEMA bronze');

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'silver')
    EXEC('CREATE SCHEMA silver');

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'gold')
    EXEC('CREATE SCHEMA gold');
GO


---------------------------------------------------bronze layer Creation------------------------------------------------------------------


---CRM Customer Info Table

if OBJECT_ID('bronze.crm_cust_info', 'u') IS NOT NULL
drop table bronze.crm_cust_info;

CREATE TABLE bronze.crm_cust_info(
    cst_id INT,
    cst_key NVARCHAR(50),
    cst_first_name NVARCHAR(50),
    cst_last_name NVARCHAR(50),
    cst_material_status NVARCHAR(50),
    cst_gndr NVARCHAR(50),
    cast_created_date DATE,
);


select * from bronze.crm_cust_info;

---CRM Product Info Table

if OBJECT_ID('bronze.crm_prd_info', 'u') IS NOT NULL
drop table bronze.crm_prd_info;

CREATE TABLE bronze.crm_prd_info(
    prd_id INT,
    prd_key NVARCHAR(50),
    prd_nm NVARCHAR(50),
    prd_cost NVARCHAR(50),
    prd_line NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt DATETIME
);

select * from bronze.crm_prd_info;

---CRM Sales Details Table

if OBJECT_ID('bronze.crm_sales_details', 'u') IS NOT NULL
drop table bronze.crm_sales_details;

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

select * from bronze.crm_sales_details;


--erp location_a101 table

if OBJECT_ID('bronze.erp_loc_a101', 'u') IS NOT NULL
drop table bronze.erp_loc_a101; 

create table bronze.erp_loc_a101(
    cid NVARCHAR(50),
   cntry nvarchar(50),
    
);

select * from bronze.erp_loc_a101;


--erp customer_az12 table

if OBJECT_ID('bronze.erp_cust_az12', 'u') IS NOT NULL
drop table bronze.erp_cust_az12;

create table bronze.erp_cust_az12(
    cid NVARCHAR(50),
    bdate DATE,
    gen NVARCHAR(50)
);

select * from bronze.erp_cust_az12;

--ERP px_cat_g1v2 table

if OBJECT_ID('bronze.erp_px_cat_g1v2', 'u') IS NOT NULL
drop table bronze.erp_px_cat_g1v2; 

create table bronze.erp_px_cat_g1v2(
    id NVARCHAR(50),
    cat NVARCHAR(50),
    subcat NVARCHAR(50),
    maintenance NVARCHAR(50)
);

select * from bronze.erp_px_cat_g1v2;
GO



--------------------------------------------Data Ingestion into Bronze Layer---------------------------------------------------------------
exec bronze.load_bronze;
GO

CREATE or ALTER PROCEDURE bronze.load_bronze as 
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
-----data insertion into bronze.crm_cust_info tables
    BEGIN TRY
        PRINT '===============================================';
        PRINT 'Loading the bronze layer';
        PRINT '===============================================';

        PRINT '-----------------------------------------------';
        PRINT 'Loading the CRM Tables';
        PRINT '-----------------------------------------------';

        
        -----data insertion into bronze.crm_cust_info tables

        set @batch_start_time = GETDATE();

        PRINT '-----------------------------------------------';
        PRINT '>>Truncating Table: bronze.crm_cust_info';
        print '-----------------------------------------------';
        
        set @start_time = GETDATE();
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT '-----------------------------------------------';
        PRINT '>>Inserting Data into: bronze.crm_cust_info';
        PRINT '-----------------------------------------------';
        BULK INSERT bronze.crm_cust_info
        from 'D:\Projects\MS SQL & MSBI BY GURU PRASAD\Data Warehouse\git_repo\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH
        (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        set @end_time = GETDATE();
        PRINT '-----------------------------------------------';
        PRINT 'Load Duration:'+ CAST(DATEDIFF(seconds,@start_time,@end_time ) AS NVARCHAR) + ' seconds';
        PRINT '-----------------------------------------------';

        --select * from bronze.crm_cust_info;
        select count(*) from bronze.crm_cust_info;
        
        PRINT 'Time taken to load CRM Customer Info Table: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -----data insertion into bronze.crm_prd_info tables

        PRINT '-----------------------------------------------';
        PRINT '>>Truncating Table: bronze.crm_prd_info';
        PRINT '-----------------------------------------------';
        set @start_time = GETDATE();
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT '-----------------------------------------------';
        PRINT '>>Inserting Data into: bronze.crm_prd_info';
        PRINT '-----------------------------------------------';
        BULK INSERT bronze.crm_prd_info
        from 'D:\Projects\MS SQL & MSBI BY GURU PRASAD\Data Warehouse\git_repo\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH
        (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        set @end_time = GETDATE();
        PRINT '-----------------------------------------------';
        PRINT 'Load Duration:'+ CAST(DATEDIFF(seconds,@start_time,@end_time ) AS NVARCHAR) + ' seconds';
        PRINT '-----------------------------------------------';
        
        --select * from bronze.crm_prd_info;
        select count(*) from bronze.crm_prd_info;

        -----data insertion into bronze.crm_sales_details tables
        
        PRINT '-----------------------------------------------';
        PRINT '>>Truncating Table: bronze.crm_sales_details';
        PRINT '-----------------------------------------------';
        set @start_time = GETDATE();
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT '-----------------------------------------------';
        PRINT '>>Inserting Data into: bronze.crm_sales_details';
        PRINT '-----------------------------------------------';
        BULK INSERT bronze.crm_sales_details
        from 'D:\Projects\MS SQL & MSBI BY GURU PRASAD\Data Warehouse\git_repo\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH
        (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        set @end_time = GETDATE();
        PRINT '-----------------------------------------------';
        PRINT 'Load Duration:'+ CAST(DATEDIFF(seconds,@start_time,@end_time ) AS NVARCHAR) + ' seconds';
        PRINT '-----------------------------------------------';

        --select * from bronze.crm_sales_details;
        select count(*) from bronze.crm_sales_details;  
        
        PRINT 'Time taken to load CRM Tables: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';


        PRINT '-----------------------------------------------';
        PRINT 'Loading the ERP Tables';
        PRINT '-----------------------------------------------';


        -----data insertion into bronze.erp_loc_a101 tables
        
        PRINT '-----------------------------------------------';
        PRINT '>>Truncating Table: bronze.erp_loc_a101';
        PRINT '-----------------------------------------------';
        set @start_time = GETDATE();
        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT '-----------------------------------------------';
        PRINT '>>Inserting Data into: bronze.erp_loc_a101';
        PRINT '-----------------------------------------------';
        BULK INSERT bronze.erp_loc_a101
        from 'D:\Projects\MS SQL & MSBI BY GURU PRASAD\Data Warehouse\git_repo\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
        WITH
        (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        set @end_time = GETDATE();
        PRINT '-----------------------------------------------';
        PRINT 'Load Duration:'+ CAST(DATEDIFF(seconds,@start_time,@end_time ) AS NVARCHAR) + ' seconds';
        PRINT '-----------------------------------------------';

        --select * from bronze.erp_loc_a101;
        select count(*) from bronze.erp_loc_a101;

        -----data insertion into bronze.erp_cust_az12 tables

        PRINT '-----------------------------------------------';
        PRINT '>>Truncating Table: bronze.erp_cust_az12';   
        PRINT '-----------------------------------------------';
        set @start_time = GETDATE();
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT '-----------------------------------------------';
        PRINT '>>Inserting Data into: bronze.erp_cust_az12';
        PRINT '-----------------------------------------------';
        BULK INSERT bronze.erp_cust_az12
        from 'D:\Projects\MS SQL & MSBI BY GURU PRASAD\Data Warehouse\git_repo\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
        WITH
        (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        set @end_time = GETDATE();
        PRINT '-----------------------------------------------';
        PRINT 'Load Duration:'+ CAST(DATEDIFF(seconds,@start_time,@end_time ) AS NVARCHAR) + ' seconds';
        PRINT '-----------------------------------------------';

        --select * from bronze.erp_cust_az12;
        select count(*) from bronze.erp_cust_az12;

        -----data insertion into bronze.erp_px_cat_g1v2 tables

        PRINT '-----------------------------------------------';
        PRINT '>>Truncating Table: bronze.erp_px_cat_g1v2';
        PRINT '-----------------------------------------------';
        set @start_time = GETDATE();
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT '-----------------------------------------------';
        PRINT '>>Inserting Data into: bronze.erp_px_cat_g1v2';
        PRINT '-----------------------------------------------';
        BULK INSERT bronze.erp_px_cat_g1v2
        from 'D:\Projects\MS SQL & MSBI BY GURU PRASAD\Data Warehouse\git_repo\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
        WITH
        (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        set @end_time = GETDATE();
        PRINT '-----------------------------------------------';
        PRINT 'Load Duration:'+ CAST(DATEDIFF(seconds,@start_time,@end_time ) AS NVARCHAR) + ' seconds';
        PRINT '-----------------------------------------------';
        
        set @batch_end_time = GETDATE();
        PRINT '===============================================';
        print 'Loading Bronze Layer Completed'
        print 'Total Duration to load Bronze Layer: '+ CAST(DATEDIFF(seconds,@batch_start_time,@batch_end_time ) AS NVARCHAR) + ' seconds';
        PRINT '===============================================';


        --select * from bronze.erp_px_cat_g1v2;
        select count(*) from bronze.erp_px_cat_g1v2;
    END TRY
    BEGIN CATCH
        PRINT 'Error Occurred While Loading the Bronze Layer';
        PRINT 'Error Number: ' +  CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error Message: ' + CAST(ERROR_STATE() AS NVARCHAR);
    END CATCH;

END
GO


