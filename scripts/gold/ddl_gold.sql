use DataWarehouse;
GO

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





SELECT
    ci.cst_id as customer_id,
    ci.cst_key as customer_number,
    ci.cst_first_name as first_name,
    ci.cst_last_name as last_name,
    ci.cst_material_status as marital_status,
    case when ci.cst_gndr != 'n/a' then ci.cst_gndr
        else coalesce(ca.gen,'n/a')
    end as new_gen,
    ci.cast_created_date,
    ca.bdate,
    la.cntry
from silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca 
on        ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
on        ci.cst_key = la.cid
