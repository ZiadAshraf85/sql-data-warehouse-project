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
    EXEC silver.load_silver;
===============================================================================
*/

create or alter procedure silver.load_silver as 
begin
DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
BEGIN TRY
SET @batch_start_time = GETDATE();
PRINT '================================================';
PRINT 'Loading Silver Layer';
PRINT '================================================';

PRINT '------------------------------------------------';
PRINT 'Loading CRM Tables';
PRINT '------------------------------------------------';

SET @start_time = GETDATE();

-- Loading silver.crm_cust_info
print'>>truncate table: silver.crm_cust_info'
truncate table silver.crm_cust_info
print'>>INSERT Data silver.crm_cust_info'

INSERT INTO silver.crm_cust_info (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
)
SELECT 
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,
    CASE 
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        ELSE 'n/a'
    END AS cst_marital_status,-- Normalize marital status values to readable format
    CASE 
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'n/a'
    END AS cst_gndr,-- Normalize gender values to readable format
    cst_create_date
FROM (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY cst_id 
            ORDER BY cst_create_date DESC
        ) AS flag_last
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
) AS t
WHERE t.flag_last = 1; -- Select the most recent record per customer

SET @end_time = GETDATE();
PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
PRINT '>> -------------';

PRINT '------------------------------------------------';

SET @start_time = GETDATE();
-- Loading silver.crm_prd_info

print'>>truncate table: silver.crm_prd_info'
truncate table silver.crm_prd_info
print'>>INSERT Data silver.crm_prd_info'

insert into silver.crm_prd_info(
    prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt      
 )
select 
prd_id,
replace(substring(prd_key,1,5),'-','_') as cat_id,
SUBSTRING(prd_key,7,len(prd_key)) as prd_key,
prd_nm,
isnull(prd_cost,0),
case when UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
     when UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
     when UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
     when UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
     else 'n/a'
end as prd_line,
cast(prd_start_dt as date ),
cast(lead(prd_start_dt) over(PARTITION by prd_key order by prd_start_dt)-1 as DATE) as pred_end_dt
from bronze.crm_prd_info
-- to check if any addition item.
--where SUBSTRING(prd_key,7,len(prd_key))  in (

--select distinct sls_prd_key from bronze.crm_sales_details)

SET @end_time = GETDATE();
PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
PRINT '>> -------------';

SET @start_time = GETDATE();
-- Loading silver.crm_sales_details
print'>>truncate table: silver.crm_sales_details'
truncate table silver.crm_sales_details
print'>>INSERT Data silver.crm_sales_details'
insert into silver.crm_sales_details(
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
select
sls_ord_num,
sls_prd_key,
sls_cust_id,
case when sls_order_dt=0 or len(sls_order_dt)!=8 then NULL
     else cast(cast(sls_order_dt as varchar) as date)
     end as sls_order_dt ,

case when sls_ship_dt=0 or len(sls_ship_dt)!=8 then NULL
     else cast(cast(sls_ship_dt as varchar) as date)
     end as sls_ship_dt ,

case when sls_due_dt=0 or len(sls_due_dt)!=8 then NULL
     else cast(cast(sls_due_dt as varchar) as date)
     end as sls_due_dt,

case when sls_sales<=0 or sls_sales is null or sls_sales !=sls_quantity* abs(sls_price) 
          then sls_quantity* abs(sls_price) 
          else sls_sales
          end as sls_sales,

sls_quantity,
CASE 
        WHEN sls_price IS NULL OR sls_price <= 0
            THEN sls_sales / NULLIF(sls_quantity, 0)
        ELSE sls_price
    END AS sls_price
from bronze.crm_sales_details

SET @end_time = GETDATE();
PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
PRINT '>> -------------';

SET @start_time = GETDATE();
-- Loading silver.erp_cust_az12

print'>>truncate table: silver.erp_cust_az12'
truncate table silver.erp_cust_az12
print'>>INSERT Data silver.erp_cust_az12'

insert into silver.erp_cust_az12(
cid,
bdate,
gen

)
select 
case when cid like 'NAS%' then substring(cid,4,len(cid))
    else cid
    end as cid,
case when bdate>getdate() then NULL
    else bdate
    end as bdate,
case 
     when upper(trim(gen)) in ('F','FEMALE') then 'Female'
     when upper(trim(gen)) in ('M','MALE') then 'Male'
     else 'n/a'
     end as gen
from bronze.erp_cust_az12

SET @end_time = GETDATE();
PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
PRINT '>> -------------';

SET @start_time = GETDATE();
--Loading silver.erp_loc_a101
print'>>truncate table: silver.erp_loc_a101'
truncate table silver.erp_loc_a101
print'>>INSERT Data silver.erp_loc_a101'
insert into silver.erp_loc_a101(
cid,
cntry

)
select 
replace(cid,'-','') as cid,
case when trim(cntry)='DE' then 'Germany' 
     when trim(cntry)='' or cntry is null then 'n/a'
     when trim(cntry) in ('USA','US') then 'United States'
     else trim(cntry)
     end as cntry
from bronze.erp_loc_a101

SET @end_time = GETDATE();
PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
PRINT '>> -------------';

SET @start_time = GETDATE();
--Loading silver.erp_px_cat_g1v2
print'>>truncate table: silver.erp_px_cat_g1v2'
truncate table silver.erp_px_cat_g1v2
print'>>INSERT Data silver.erp_px_cat_g1v2'
insert into silver.erp_px_cat_g1v2 (
id,
cat,
subcat,
maintenance
		)
select id,
cat,
subcat,
maintenance
from bronze.erp_px_cat_g1v2;

SET @end_time = GETDATE();
PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
PRINT '>> -------------';

		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
end

--EXEC silver.load_silver;