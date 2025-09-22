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

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO
-- after i show all of this data i will choose the crm_cust_info as a master of this tables
create view gold.dim_customers AS
select
	ROW_NUMBER() over(order by cst_id) as customer_key,
	ci.cst_id customer_id,
	ci.cst_key customer_number,
	ci.cst_firstname first_name,
	ci.cst_lastname last_name,
	lo.cntry country,
	ci.cst_marital_status marital_status,
	case when ci.cst_gndr !='n/a' then ci.cst_gndr
	else COALESCE(ca.gen,'n/a')
	end as gender,
	ca.bdate birthdate, 
	ci.cst_create_date create_date
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on ci.cst_key=ca.cid
left join silver.erp_loc_a101 lo
on ci.cst_key=lo.cid
GO

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO
create view gold.dim_products AS
select
    ROW_NUMBER() OVER (order by pn.prd_start_dt, pn.prd_key) as product_key, -- Surrogate key
    pn.prd_id        product_id,
    pn.prd_key       product_number,
    pn.prd_nm        product_name,
    pn.cat_id        category_id,
    pc.cat           category,
    pc.subcat        subcategory,
    pc.maintenance   maintenance,
    pn.prd_cost      cost,
    pn.prd_line      product_line,
    pn.prd_start_dt  start_date
from silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 pc
    on pn.cat_id = pc.id
where pn.prd_end_dt is null; -- Filter out all historical data

GO

-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO
create view gold.fact_sales AS
select
    sd.sls_ord_num   order_number,
    pr.product_key   product_key,
    cu.customer_key  customer_key,
    sd.sls_order_dt  order_date,
    sd.sls_ship_dt   shipping_date,
    sd.sls_due_dt    due_date,
    sd.sls_sales     sales_amount,
    sd.sls_quantity  quantity,
    sd.sls_price     price
from silver.crm_sales_details sd
left join gold.dim_products pr
    on sd.sls_prd_key = pr.product_number
left join gold.dim_customers cu
    on sd.sls_cust_id = cu.customer_id;

GO

-- RUN views 
SELECT * 
FROM gold.dim_customers;

SELECT * 
FROM gold.dim_products;

SELECT * 
FROM gold.fact_sales;
