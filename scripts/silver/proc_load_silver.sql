use DataWarehouse;
GO

/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/



-- EXEC silver.load_silver;
-- GO


CREATE or ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';




    /*===================================================Insert into Silver Layer Tables===============================================*/

    ----------------------------------------------------Insert into silver.crm_cust_info --------------------------------------------------
    SET @start_time = GETDATE();
    PRINT '>>Truncating Table: silver.crm_cust_info';
    TRUNCATE TABLE silver.crm_cust_info;
    PRINT '>>Inserting Data into: silver.crm_cust_info';
    INSERT INTO silver.crm_cust_info(
        cst_id,
        cst_key,
        cst_first_name,
        cst_last_name,
        cst_material_status,
        cst_gndr,
        cast_created_date
    )
    SELECT 
    cst_id,
    cst_key,
    trim(cst_first_name) as cst_first_name,
    trim(cst_last_name) as cst_last_name,
    case 
        when UPPER(TRIM(cst_material_status)) = 'S' then 'Single'
        when UPPER(TRIM(cst_material_status)) = 'M' then 'Married'
        else 'n/a'
    end as cst_material_status,-- Normalize marital status values to readable format

    case 
        when UPPER(TRIM(cst_gndr)) = 'M' then 'Male'
        when UPPER(TRIM(cst_gndr)) = 'F' then 'Female'
        else 'n/a'
    end as cst_gndr_std,                                        -- Normalize gender values to readable format

    cast_created_date
    from (
    select *,
    ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cast_created_date DESC) AS flag_last
    from bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
    ) AS A                                                  -- Select the most recent record per customer
    WHERE flag_last = 1;
    SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
    

    ------------------------------------------------Check Data in Origin(Bronze Layer)Table-------------------------------------------
    
    
    /* 
    ----------------------------------------------------------------------------------
    --Check for Nulls or Duplicates in primary key
    --Expection : No Results
    ----------------------------------------------------------------------------------

    --select * from bronze.crm_prd_info;
    select 
    prd_id,
    COUNT(*) AS cnt
    from bronze.crm_prd_info
    GROUP BY prd_id
    HAVING COUNT(*) > 1 or prd_id IS NULL ;
    GO


    -----------------------------
    --Check for Unwanter Spaces
    --Expection : No Results
    -----------------------------

    select prd_nm from bronze.crm_prd_info
    where prd_nm != trim(prd_nm);
    GO


    -----------------------------
    --Check for Nulls or Negative Numbers
    --Expection : No Results
    -----------------------------

    select prd_cost from bronze.crm_prd_info
    where prd_cost  < 0 or prd_cost IS NULL;
    GO




    -------------------------------------------------
    --Data Standardization & Consistency
    -------------------------------------------------

    --for gender column
    SELECT distinct cst_gndr from silver.crm_cust_info;

    --for material status column
    SELECT distinct cst_material_status from silver.crm_cust_info;
    GO

    -------------------------------------------------
    --Check for Invalid Date orders
    -------------------------------------------------

    select prd_start_dt, prd_end_dt from bronze.crm_prd_info
    where prd_end_dt < prd_start_dt;
    GO


    */




    ----------------------------------------------------Insert into silver.crm_prd_info --------------------------------------------------
    
    SET @start_time = GETDATE();
    PRINT '>>Truncating Table: silver.crm_prd_info';
    TRUNCATE TABLE silver.crm_prd_info;
    PRINT '>>Inserting Data into: silver.crm_prd_info';
    INSERT INTO silver.crm_prd_info(
        prd_id,
        cat_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
    )
    SELECT 
    prd_id,
    replace(SUBSTRING(prd_key,1,5),'-','_') as cat_id,  --- Extract and format category ID from product key
    SUBSTRING(prd_key,7,len(prd_key)) as prd_key,       --- Extract product key without category prefix
    prd_nm,
    ISNULL(prd_cost,0) as prd_cost,                     --- Replace null product costs with 0
    case UPPER(TRIM(prd_line))
        when  'M' then 'Mountain'
        when  'R' then 'Road'
        when  'S' then 'Other Sales'
        when  'T' then 'Touring'
        else 'n/a'
    end as prd_line,                                   --- Map product line codes to descriptive names 
    cast(prd_start_dt as date) as prd_start_dt,
    cast(
        LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 as date
        ) AS prd_end_dt                                --- Set end date as day before next start date per product
    from bronze.crm_prd_info;
    SET @end_time = GETDATE();
    PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
    PRINT '>> -------------';

    SELECT * from silver.crm_prd_info;


   SELECT * from bronze.crm_prd_info;
--sp_help 'bronze.crm_prd_info'



----------------------------------------Check Data Quality in bronze Layer CRM Product Info Table  -----------------------------------------

    /*

    ----------------------------------------
    --check for invalid dates of order date
    ----------------------------------------

    select 
    nullif(sls_order_dt,0) as sls_order_dt
    from bronze.crm_sales_details
    where sls_order_dt <= 0 
    or len(sls_order_dt) <> 8 
    or sls_order_dt > 20500101 
    or sls_order_dt < 19000101;






    --Check Integrity of sls_prd_key and sls_cust_id with product and customer tables respectively


    SELECT 
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,  
    sls_quantity,
    sls_price
    from bronze.crm_sales_details
    where sls_prd_key not in (select prd_key from silver.crm_prd_info) ;



    SELECT 
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,  
    sls_quantity,
    sls_price
    from bronze.crm_sales_details
    where sls_cust_id not in (select cst_id from silver.crm_cust_info) ;


    */






    ----------------------------------------------------Insert into silver.crm_sales_details --------------------------------------------------

    set @start_time = GETDATE();
    PRINT '>>Truncating Table: silver.crm_sales_details';
    TRUNCATE TABLE silver.crm_sales_details;
    PRINT '>>Inserting Data into: silver.crm_sales_details';
    INSERT into silver.crm_sales_details(
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price
    )
    SELECT 
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,

    case when sls_order_dt <= 0 or len(sls_order_dt) <> 8 then null
    else cast(cast(sls_order_dt as varchar) as date)
    end as sls_order_dt,                             -- Set invalid order dates to null 
                                                    
    case when sls_ship_dt <= 0 or len(sls_ship_dt) <> 8 then null
    else cast(cast(sls_ship_dt as varchar) as date)
    end as sls_ship_dt,                              -- Set invalid shipping dates to null
                                                    
    case when sls_due_dt <= 0 or len(sls_due_dt) <> 8 then null
    else cast(cast(sls_due_dt as varchar) as date)
    end as sls_due_dt,                                -- Set invalid due dates to null

    case when sls_sales IS NULL OR sls_sales <= 0 or sls_sales <> sls_quantity * ABS(sls_price ) 
            then sls_quantity * ABS(sls_price )
            else sls_sales 
        end as sls_sales,                              -- Recalculate sales if null, negative, or inconsistent with quantity and price

    sls_quantity,

    case when sls_price IS NULL OR sls_price <= 0 
            then ABS(sls_sales / NULLIF(sls_quantity,0))
            else ABS(sls_price)
        end as sls_price                            -- Recalculate price if null or negative
    from bronze.crm_sales_details;
    set @end_time = GETDATE();
    PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
    PRINT '>> -------------';                      



    SELECT * from silver.crm_sales_details;




    -----------------------------------------Check Data Quality in Silver Layer CRM Product Info Table  -----------------------------------------

    ----------------------------------------
    --check for invalid dates of order date
    ----------------------------------------

    -- select 
    -- nullif(sls_order_dt,0) as sls_order_dt
    -- from silver.crm_sales_details
    -- where sls_order_dt <= 0 
    -- or len(sls_order_dt) <> 8 
    -- or sls_order_dt > 20500101 
    -- or sls_order_dt < 19000101;
    
    --or (due to error execution the above code modified below)

    SELECT 
    sls_order_dt,
    NULLIF(CONVERT(CHAR(8), sls_order_dt, 112), '00000000') AS sls_order_dt_fmt
    FROM silver.crm_sales_details
    WHERE sls_order_dt IS NULL
    OR LEN(CONVERT(CHAR(8), sls_order_dt, 112)) <> 8
    OR CONVERT(INT, CONVERT(CHAR(8), sls_order_dt, 112)) > 20500101
    OR CONVERT(INT, CONVERT(CHAR(8), sls_order_dt, 112)) < 19000101;

    -------------------------------------------
    --check for invalid dates of shipping date
    -------------------------------------------

    select 
    nullif(sls_ship_dt,0) as sls_ship_dt
    from bronze.crm_sales_details
    where sls_ship_dt <= 0 
    or len(sls_ship_dt) <> 8 
    or sls_ship_dt > 20500101 
    or sls_ship_dt < 19000101;

    -------------------------------------------
    --check for invalid dates of due date
    -------------------------------------------

    select 
    nullif(sls_due_dt,0) as sls_ship_dt
    from bronze.crm_sales_details
    where sls_due_dt <= 0 
    or len(sls_due_dt) <> 8 
    or sls_due_dt > 20500101 
    or sls_due_dt < 19000101;

    --------------------------------
    --Check for Invalid Date orders
    --------------------------------

    select * from bronze.crm_sales_details
    where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt;


    ---------------------------------------------------------------
    -- Check Data Consistency: Between Sales, Quantity, and Price
    -- >> Sales = Quantity * Price
    -- >> Values must not be NULL, zero, or negative.
    ----------------------------------------------------------------

    SELECT DISTINCT
        sls_sales as old_sls_sales,
        sls_quantity,
        sls_price as old_sls_price,
        case when sls_sales IS NULL OR sls_sales <= 0 or sls_sales <> sls_quantity * ABS(sls_price ) 
            then sls_quantity * ABS(sls_price )
            else sls_sales 
        end as sls_sales,
        case when sls_price IS NULL OR sls_price <= 0 
            then ABS(sls_sales / NULLIF(sls_quantity,0))
            else ABS(sls_price)
        end as sls_price
    FROM bronze.crm_sales_details
    WHERE 
        sls_sales IS NULL OR sls_sales <= 0
        OR sls_quantity IS NULL OR sls_quantity <= 0
        OR sls_price IS NULL OR sls_price <= 0
        OR sls_sales <> sls_quantity * sls_price
        ORDER BY sls_sales, sls_quantity, sls_price;


    select * from bronze.crm_sales_details;

    --------------------------------------------------------Insert into silver.erp_cust_az12 --------------------------------------------------

    set @start_time = GETDATE();
    PRINT '>>Truncating Table: silver.erp_cust_az12';
    TRUNCATE TABLE silver.erp_cust_az12;
    PRINT '>>Inserting Data into: silver.erp_cust_az12';
    insert into silver.erp_cust_az12(
        cid,
        bdate,
        gen
    )
    select 
    case when cid like 'NAS%' then SUBSTRING(cid,4,len(cid))
        else cid
    end as cst_id,                                          -- Remove 'NAS' prefix if present      
    case when bdate > GETDATE() then null
        else bdate
    end as bdate,                                           -- set future birthdates to null
    case when UPPER(TRIM(gen)) in ('M','Male') then 'Male'
        when UPPER(TRIM(gen)) in ('F','Female') then 'Female'
        else 'n/a'
    end as gen                                              -- Normalize the gender values and set invalids to 'n/a'
    from bronze.erp_cust_az12;
    set @end_time = GETDATE();
    PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
    PRINT '>> -------------';


    SELECT * from silver.erp_cust_az12;

    select * from bronze.erp_cust_az12;






    ---------------------------------------Check Data Quality in Silver Layer ERP Customer az12 Table  -----------------------------------------


    /*
    ----------------------------------------------------------------------
    --Check CID with cst_ky column of crm_cust_info table  for integrity
    --Result : No Records 
    ----------------------------------------------------------------------


    select 
    cid,
    case when cid like 'NAS%' then SUBSTRING(cid,4,len(cid))
        else cid
    end as cst_id,
    bdate,
    gen
    from bronze.erp_cust_az12
    WHERE case when cid like 'NAS%' then SUBSTRING(cid,4,len(cid))
        else cid
    end NOT IN (select cst_key from silver.crm_cust_info);
    GO


    select distinct bdate from bronze.erp_cust_az12
    where bdate < '1924-01-01' or bdate > GETDATE();


    -------------------------------------------------
    --Data Standardization & Consistency
    -------------------------------------------------

    --for gender column
    SELECT distinct gen from bronze.erp_cust_az12;

    */



    ----------------------------------------------------------------------
    --Check CID with cst_ky column of crm_cust_info table  for integrity
    --Result : No Records 
    ----------------------------------------------------------------------


    select 
    cid,
    case when cid like 'NAS%' then SUBSTRING(cid,4,len(cid))
        else cid
    end as cst_id,
    bdate,
    gen
    from silver.erp_cust_az12
    WHERE case when cid like 'NAS%' then SUBSTRING(cid,4,len(cid))
        else cid
    end NOT IN (select cst_key from silver.crm_cust_info);


    select distinct bdate from silver.erp_cust_az12
    where bdate < '1924-01-01' or bdate > GETDATE();


    -------------------------------------------------
    --Data Standardization & Consistency
    -------------------------------------------------

    --for gender column
    SELECT distinct gen from silver.erp_cust_az12;





    --------------------------------------------------------Insert into silver.erp_loc_az12 --------------------------------------------------

    set @start_time = GETDATE();
    PRINT '>>Truncating Table: silver.erp_loc_a101';
    TRUNCATE TABLE silver.erp_loc_a101;
    PRINT '>>Inserting Data into: silver.erp_loc_a101';
    insert into silver.erp_loc_a101(
        cid,
        cntry
    )
    SELECT 
    REPLACE(cid,'-','') as cid,                             -- Remove hyphens from CID for consistency
    case 
        when TRIM(cntry) = '' or cntry is null then 'n/a'
        when TRIM(cntry) in ('US','USA') then 'United States'
        when TRIM(cntry) = 'DE' then 'Germany'
        else cntry
    end as cntry                                             -- Standardize country names and handle missing or balnk values           
    from bronze.erp_loc_a101;
    set @end_time = GETDATE();
    PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
    PRINT '>> -------------';


    select * from silver.erp_loc_a101;



    ---------------------------------------Check Data Quality in Silver Layer ERP location a101 Table  -----------------------------------------


    -------------------------------------------------
    --Data Standardization & Consistency
    -------------------------------------------------
    /*
    --for gender column
    SELECT distinct cntry as old_cntry,
    case 
        when TRIM(cntry) = '' or cntry is null then 'n/a'
        when TRIM(cntry) in ('US','USA') then 'United States'
        when TRIM(cntry) = 'DE' then 'Germany'
        else cntry
    end as cntry
    from bronze.erp_loc_a101;

    */


    SELECT 
    REPLACE(cid,'-','') as cid,
    cntry
    from bronze.erp_loc_a101
    WHERE REPLACE(cid,'-','') NOT IN (SELECT cst_key from silver.crm_cust_info);

    --for gender column
    SELECT distinct cntry
    from silver.erp_loc_a101
    ORDER BY cntry;




    SELECT cst_key from bronze.crm_cust_info;





    --------------------------------------------------------Insert into silver.erp_px_cat_g1v2 --------------------------------------------------
    set @start_time = GETDATE();
    PRINT '>>Truncating Table: silver.erp_px_cat_g1v2';
    TRUNCATE TABLE silver.erp_px_cat_g1v2;
    PRINT '>>Inserting Data into: silver.erp_px_cat_g1v2';
    INSERT into silver.erp_px_cat_g1v2
    (
        id,
        cat,
        subcat,
        maintenance 
    )
    SELECT 
    id,
    cat,
    subcat,
    maintenance
    from bronze.erp_px_cat_g1v2;
    set @end_time = GETDATE();
    PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
    PRINT '>> -------------';

    SELECT * from silver.erp_px_cat_g1v2;

    SELECT * from bronze.erp_px_cat_g1v2;


    -----------------------------------Check Data Quality in Silver Layer ERP location a101 Table  --------------------------------

    -----------------------------
    --Check for unwanted spaces
    -----------------------------

    SELECT * from bronze.erp_px_cat_g1v2
    where cat != trim(cat) or subcat != trim(subcat) or maintenance != trim(maintenance);



    -------------------------------------------------
    --Data Standardization & Consistency
    -------------------------------------------------


    SELECT 
    distinct cat
    from bronze.erp_px_cat_g1v2;

    SELECT 
    distinct subcat
    from bronze.erp_px_cat_g1v2;


    SELECT 
    distinct maintenance
    from bronze.erp_px_cat_g1v2;
    

    SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Silver Layer is Completed';
        PRINT '- Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
		
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END
GO