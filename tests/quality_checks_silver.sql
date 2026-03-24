/*
=====================================================================================================
Quality checks
=====================================================================================================
Script purpose:
	Perform various quality checks for data consistency, accuracy, and standarization 
	across the "silver' schema. This script includes checks for:
	- Null or duplicate PK.
	- Unwanted leading or trailing spaces in string fields.
	- Data standarization and consistency.
	- Invalid date ranges and orders.
	- Data consistency between related fields.

Use and Notes:
	- Run these checks after data loading Silver Layer. 
	- Investigate and resolve any discrepancies found during these checks.

=====================================================================================================
*/
	print 'SILVER.crm_cust_info'

		-- Find duplicates
		-- Expectation: no results
		select cst_id, count(*)
		from silver.crm_cust_info t 
		group by t.cst_id
		having count(*) > 1 or t.cst_id is null -- No results OK

		SELECT 'Action'='remove duplicates', r.* 
		FROM (
				select * , row_number() over( partition by t.cst_id  order by t.cst_create_date desc) flag_last
				from silver.crm_cust_info t 
			) R
		WHERE r.flag_last !=1 	--> No results OK

		-- Check for unwanted spaces  
		-- Expectation: No results
		select t.cst_firstname
		from silver.crm_cust_info t 
		where t.cst_firstname != trim(t.cst_firstname) --> OK

		-- Expectation: No results
		select t.cst_lastname
		from silver.crm_cust_info t 
		where t.cst_lastname != trim(t.cst_lastname) --> OK

		-- Expectation: No results
		select t.cst_gndr
		from silver.crm_cust_info t 
		where t.cst_gndr != trim(t.cst_gndr) --> no results OK

		-- Expectation: No results
		select t.cst_key
		from silver.crm_cust_info t 
		where t.cst_key != trim(t.cst_key)  --> no results OK

		-- Data standarization & consistency
		select distinct t.cst_gndr
		from silver.crm_cust_info t 
		-- Data standarization & consistency
		select distinct t.cst_marital_status
		from silver.crm_cust_info t 

		-- Expectation: no results
		select * 
		from silver.crm_cust_info t where t.cst_key='PO25' 

		-- See final new records
		select * from silver.crm_cust_info 



	print 'SILVER.crm_prd_info'

		-- Check for unwanted spaces
		-- Expectation: no results
		select prd_nm
		from silver.crm_prd_info t
		where t.prd_nm != trim(prd_nm) -- OK passed

		-- Check for NULLs or negative numbers
		-- Expectation: no results
		select prd_cost, *
		from silver.crm_prd_info t
		where prd_cost < 0 or prd_cost is null

		-- Data standarization & consistency. Asl expert about meaning
		select distinct prd_line
		from silver.crm_prd_info t

		-- Check for unwanted spaces
		-- Expectation: no results
		select prd_line
		from silver.crm_prd_info t
		where t.prd_line != trim(prd_line) or prd_line is null -- no results OK

		-- Check for invalid Date Orders, End Date should not be earlier than start date
		-- Expectation: no results
		select *
		from silver.crm_prd_info t
		where prd_end_dt < prd_start_dt
		print 'there is an issue about these dates... after checking with business experts proceed to fix date ranges.'
		print 'analyze a couple of products to build logic'

		-- SOLUTION: based on Start Date build End Date field (substract a day from candidate end date)
		select prd_id, prd_key, prd_nm
			, prd_start_dt, prd_end_dt
			--, lead(prd_start_dt) over (partition by prd_key order by prd_start_dt asc) - 1 'prd_end_dt_test'
		from silver.crm_prd_info t
		where t.prd_key in ('HL-U509-R', 'HL-U509')

		-- Check results
		SELECT * FROM silver.crm_prd_info  

--=================================================================================================
	Print 'SILVER.crm_sales_details'

		PRINT 'Verify resultes with expectations.........'

		--Expectation: no results
		select *
		from silver.crm_sales_details t
		where sls_ord_num != trim(sls_ord_num) --> OK

		-- Expectation: no results
		select sls_prd_key, *
		from silver.crm_sales_details t
		where sls_prd_key not in (select t.prd_key from SILVER.crm_prd_info t) -- OK!

		-- Expectation: no results
		select sls_cust_id, *
		from silver.crm_sales_details t
		where sls_cust_id not in (select t.cst_id from SILVER.crm_cust_info t) -- OK!	

		-- Expectation: no results
		select sls_order_dt  
		from silver.crm_sales_details t
		where 
			 sls_order_dt > cast('20500101' as date)
			or  sls_order_dt < cast( '19000101' as date)

		-- Convert date fields to valid dates
		-- Expectation: no results
		select sls_ship_dt 
		from silver.crm_sales_details t
		where 
			 sls_ship_dt > cast('20500101' as date)
			or  sls_ship_dt < cast( '19000101' as date)

		-- Convert date fields to valid dates
		-- Expectation: no results
		select sls_due_dt  
		from silver.crm_sales_details t 
		where 
			 sls_due_dt  > cast('20500101' as date) 
			or  sls_due_dt < cast( '19000101' as date) 

		-- Check invalid Date Orders
		-- Endure that order date < ship date < due date
		select sls_order_dt, sls_ship_dt, sls_due_dt
		from silver.crm_sales_details t 
		WHERE sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt

	print 'business rule: Sales = Quatity x Price
			Negative, zeros, null are NOT ALLOWED!'  --> 26h 10min
	print 'RULES: if Sales is negative, or null, derive it using Quantity x Price
			If Price is zero or null, calculate it using Sales and Quantity 
			If Price is negative, convert it to positive value'

		-- Expectation: no results
		select  sls_sales 'old_sls_sales'
			, sls_quantity 'old_sls_quantity'
			, sls_price  'old_sls_price'
			, 'sls_sales_BARAA' =
				case when sls_sales is null or sls_sales < =0 or sls_sales != sls_quantity * abs(sls_price)
					then sls_quantity * abs( sls_price)
					else sls_sales end 
			, 'sls_sales'= sls_quantity * ABS( isnull(sls_price, sls_sales / nullif(sls_quantity, 0) ) )
			, 'sls_quantity' =  nullif(sls_quantity, 0)						
			, 'sls_price_BARAA'= case when sls_price is null or sls_price < =0
					then sls_sales / nullif(sls_quantity, 0)
					else sls_price end 
			, 'sls_price'=ABS( isnull(sls_price, sls_sales / nullif(sls_quantity, 0)) )
		from silver.crm_sales_details t
		where sls_sales != sls_quantity * sls_price
			or sls_sales is null or sls_quantity  is null or sls_price is null 
			or sls_sales <= 0 or sls_quantity <= 0 or sls_price	<= 0
		order by sls_sales, sls_quantity, sls_price --> OK passed

		-- Final check...
		Select * from silver.crm_sales_details --OK passed


--=================================================================================================

	Print 'SILVER.erp_cust_az12'

		print 'verify results'
			
		-- Epectation: no results
		select t.*
		from silver.erp_cust_az12 t
		where  cid  NOT IN (select distinct cst_key from SILVER.crm_cust_info) --> OK passed

		-- Expectation: no records found
		select bdate 
		from silver.erp_cust_az12 t
		where bdate > getdate()  --> OK passed

		-- Expectation: 3 value only
		select distinct t.gen
		from silver.erp_cust_az12 t --> OK passed

		select * from silver.erp_cust_az12 t  -- OK passed



--=================================================================================================
	Print 'SILVER.erp_loc_a101'

		-- Expectation: no return
		select  cid
		from silver.erp_loc_a101 t
		where cid NOT IN (select i.cst_key from silver.crm_cust_info i) -- OK passed

		-- Expectation: cardinality 7, no nulls or empty fields
		select distinct cntry
		from silver.erp_loc_a101 t
		order by cntry --> OK passed

		-- final review
		select * from silver.erp_loc_a101 t



--=================================================================================================
	Print 'SILVER.erp_px_cat_g1v2'
		
		-- Check load results.
		select *
		from silver.erp_px_cat_g1v2 t

		
