/*
===============================================================================
DDL SCript: Create Gold views
===============================================================================
Purpose:
	Create views for the Gold layer in the data warehouse.
	The Fold layer represents the final dimension or fact tables (star schema)

	Each view performs transformations and combines data from the silver layer
	to produce a clean, enriched, and business-ready dataset.

Usage:
	Gold views can be queried directly for BI, analytics or reporting.

===============================================================================
*/


--=============================================================================
-- Create Dimension: gold.dim_customers
--=============================================================================
IF object( 'gold.dim_customers', 'V' ) is not null
	DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS 
	SELECT 
		ROW_NUMBER() over(order by cst_id) 'customer_key' 
		, ci.cst_id			'customer_id'
		, ci.cst_key		'customer_number'
		, ci.cst_firstname	'first_name'
		, ci.cst_lastname	'last_name'
		, cl.cntry			'country'
		, ci.cst_marital_status	'marital_status'
		, case ci.cst_gndr	
			when 'n/a' then ( case isnull(ca.gen,'') 
								when '' then 'n/a' 
								else ca.gen end )
			else ci.cst_gndr 
			end			'gender'
		, ca.bdate		'birthdate'		
		, ci.cst_create_date 'create_date'		
	FROM silver.crm_cust_info ci
		left join silver.erp_cust_az12 ca on ci.cst_key = ca.cid
		left join silver.erp_loc_a101 cl on  ci.cst_key = cl.cid;



--=============================================================================
-- Create Dimension: gold.dim_products
--=============================================================================
IF object( 'gold.dim_products', 'V' ) is not null
	DROP VIEW gold.dim_products;
GO

 CREATE or ALTER VIEW gold.dim_products AS
	SELECT ROW_NUMBER() over( order by pn.prd_start_dt, pn.prd_key) 'product_key',
		  pn.prd_id		'product_id'
		, pn.prd_key	'product_number'
		, pn.prd_nm		'product_name'
		, pn.cat_id		'category_id'
		, pc.cat		'category'
		, pc.subcat		'subcategory'
		, pc.maintenance	'maintenance'
		, pn.prd_cost	'cost'
		, pn.prd_line	'product_line'
		, pn.prd_start_dt	'start_date'	
	FROM silver.crm_prd_info pn
		left join silver.erp_px_cat_g1v2 pc on pn.cat_id = pc.id
	WHERE pn.prd_end_dt is null 



--=============================================================================
-- Create Fact: gold.fact_sales
--=============================================================================
IF object( 'gold.fact_sales', 'V' ) is not null
	DROP VIEW gold.fact_sales;
GO

 CREATE VIEW gold.fact_sales AS
	SELECT 
		  sd.sls_ord_num	'order_number'
		, pr.product_key	'product_key'
		, cu.customer_key	'customer_key'
		, sd.sls_order_dt	'order_date'
		, sd.sls_ship_dt	'shipping_date'
		, sd.sls_due_dt		'due_date'
		, sd.sls_sales		'sales_amount'
		, sd.sls_quantity	'quantity'
		, sd.sls_price		'price'
	FROM silver.crm_sales_details sd
		left join gold.dim_products  pr on sd.sls_prd_key = pr.product_number
		left join gold.dim_customers cu on sd.sls_cust_id = cu.customer_id

