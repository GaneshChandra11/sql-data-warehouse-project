/*
========================================================DATA QUALITY CHECKS ==============================================================
*/

----------------------------------------------------------------------------------
--Check for Nulls or Nulls in primary key columns in bronze tables crm_cust_info
--Expection : No Nulls or Nulls in primary key columns
----------------------------------------------------------------------------------

select * from bronze.crm_cust_info;

select 
cst_id,
COUNT(*) AS cnt
 from bronze.crm_cust_info
 GROUP BY cst_id
 HAVING COUNT(*) > 1 or cst_id IS NULL ;
GO



--Data with no duplicates or nulls in primary key columns
SELECT * from (
select *,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cast_created_date DESC) AS flag_last
from bronze.crm_cust_info
WHERE cst_id IS NOT NULL
) AS A
WHERE flag_last = 1;
GO



-----------------------------
--Check for Unwanter Spaces
--Expection : No Results
-----------------------------

select cst_first_name from bronze.crm_cust_info
where cst_first_name != trim(cst_first_name);
GO