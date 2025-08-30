
/*
====================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
====================================================================

Script Purpose:
   This stored procedure loads data into the 'bronze' schema from external CSV files.
   It performs the following actions:
   - Truncates the bronze tables before loading data.
   - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
   None.
   This stored procedure does not accept any parameters or return any values.

Usage Example:
   EXEC bronze.load_bronze;
====================================================================
*/

use DataWarehouse;
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


