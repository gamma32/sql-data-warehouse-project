
/*
=============================================================================================
Store Procedure: Load Silver Layer [ bronze -> silver]
=============================================================================================
Script Purpose:
	This sp loads data into the "Silver" schema from "Bronze" schema
	It performs the following actions:
	- Truncates silver tables before loading
	- Uses "Bulk Insert" to load transformed bronze data into corresponding silver tables.

Parameters:
	None. This sp does not take any parameters or return any values or objects.

Usage example:
	
	EXEC silver.load_silver ;

=============================================================================================
*/
CREATE OR ALTER   PROCEDURE [silver].[load_silver] AS

BEGIN

	Declare @start_time datetime, @end_time datetime, @batch_start_time datetime
	
	Begin TRY
		PRINT '----------------------------------------------------------------------------';
		PRINT 'Loading silver Layer...';
		PRINT '----------------------------------------------------------------------------';

		PRINT '-----------------------------------------------------------------';
		PRINT 'Loading CRM tables';
		PRINT '-----------------------------------------------------------------';

		set @batch_start_time=GETDATE()

		set @start_time = GETDATE();

		PRINT '>>1 Truncating table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		Print '>> Inserting data into: silver.crm_cust_info'
		INSERT INTO silver.crm_cust_info (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)
		SELECT 
			cst_id, cst_key
			, trim(cst_firstname) 'cst_firstname'
			, trim(cst_lastname) 'cst_lastname'
			, case upper(trim(cst_marital_status)) 
				when 'M' then 'Married'
				when 'S' then 'Single' else 'n/a' end 'cst_marital_status'
			, case  upper(trim(cst_gndr))  
				when 'F' then 'Female' 
				when 'M' then 'Male' else 'n/a' end 'cst_gndr'
			, cst_create_date
		FROM (
				select * , row_number() over( partition by t.cst_id  order by t.cst_create_date desc) flag_last
				from bronze.crm_cust_info t 
			) R
		WHERE r.flag_last =1 and r.cst_id is not null

		set @end_time = GETDATE();
		PRINT '>> Load duration: ' + cast(datediff(second, @start_time,  @end_time) as varchar) + ' seconds';
		PRINT '>> -------------';


		set @start_time = GETDATE();
		PRINT '>>2 Truncating table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		Print '>> Inserting data into: silver.crm_prd_info'
		INSERT INTO silver.crm_prd_info (prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
		SELECT prd_id
			, replace(substring(prd_key, 1, 5), '-','_')	'cat_id'
			, right(prd_key, len( trim(prd_key) ) - 6 )		'prd_key'
			, prd_nm
			, isnull(prd_cost, 0) 'prd_cost'	
			, case upper( trim(prd_line) )
				when 'M' then 'Mountain' 
				when 'R' then 'Road'
				when 'S' then 'Other Sales'
				when 'T' then 'Touring'
				else 'n/a' end				'prd_line'
			, cast(prd_start_dt as date)	'prd_start_dt'	
			, cast(lead(prd_start_dt) over (partition by prd_key order by prd_start_dt asc) - 1 as date) 'prd_end_dt'
		FROM bronze.crm_prd_info t

		set @end_time = GETDATE();
		PRINT '>> Load duration: ' + cast(datediff(second, @start_time,  @end_time) as varchar) + ' seconds';
		PRINT '>> -------------';


		set @start_time = GETDATE();
		PRINT '>>3 Truncating table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		print '>> Inserting data into table: silver.crm_sales_details'
		INSERT INTO silver.crm_sales_details ( sls_ord_num, sls_prd_key, sls_cust_id
				, sls_order_dt, sls_ship_dt, sls_due_dt
				, sls_sales, sls_quantity
					, sls_price )
		SELECT 
			sls_ord_num
			, sls_prd_key
			, sls_cust_id
			, case 
					when sls_order_dt = 0 OR len(sls_order_dt) !=8 then null
					else cast(sls_order_dt as date) end  'sls_order_dt' 
			, case 
					when sls_ship_dt = 0 OR len(sls_ship_dt) !=8 then null
					else cast(sls_ship_dt as date) end  'sls_ship_dt' 
			, case 
					when sls_due_dt = 0 OR len(sls_due_dt) !=8 then null
					else cast(sls_due_dt as date) end  'sls_due_dt' 
			, case 
					when sls_sales is null or sls_sales < = 0 or sls_sales != sls_quantity * abs(sls_price)
					then sls_quantity * abs( sls_price)
					else sls_sales end 'sls_sales' --> recalculate Sales when original value is missing or incorrect
			, sls_quantity
			, case when sls_price is null or sls_price < = 0
						then sls_sales / nullif(sls_quantity, 0)
						else sls_price end 'sls_price' --> Derive price when original value is invalid
		FROM bronze.crm_sales_details t  

		set @end_time = GETDATE();
		PRINT '>> Load duration: ' + cast(datediff(second, @start_time,  @end_time) as varchar) + ' seconds';
		PRINT '>> -------------';

		PRINT '-----------------------------------------------------------------';
		PRINT 'Loading ERP tables';
		PRINT '-----------------------------------------------------------------';

		set @start_time = GETDATE();
		PRINT '>>4 Truncating table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		print '>> Inserting data into table: silver.erp_cust_az12'
		INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
		SELECT 			
			Case when cid like 'NAS%' then right(cid, len(cid)-3) --> remove NAS prefix when found
				else cid end 'cid'				
			, case when bdate > getdate() then null
				else bdate end 'bdate'				
			, case	when upper( trim(gen) ) in ('F', 'FEMALE') then 'Female'
					when upper( trim(gen) ) in ('M', 'MALE') then 'Male'
					else 'n/a'
					end 'gen' --> Normalize Gender values and handle unknown cases
		FROM bronze.erp_cust_az12 t 

		set @end_time = GETDATE();
		PRINT '>> Load duration: ' + cast(datediff(second, @start_time,  @end_time) as varchar) + ' seconds';
		PRINT '>> -------------';


		set @start_time = GETDATE();
		PRINT '>>5 Truncating table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		print '>> Inserting data into table: silver.erp_loc_a101'
		INSERT INTO silver.erp_loc_a101 (cid, cntry)
		SELECT 				
				replace(cid, '-', '') 'cid'		--> remove "-" from customer key		
			,case upper(isnull(cntry , ''))
				when 'US'  then 'United States'
				when 'USA' then 'United States'
				when 'DE'  then 'Germany'
				when ''		then 'n/a'
				else trim(cntry)
			end 'cntry'							--> standarization & consistency
		FROM bronze.erp_loc_a101 t 

		set @end_time = GETDATE();
		PRINT '>> Load duration: ' + cast(datediff(second, @start_time,  @end_time) as varchar) + ' seconds';
		PRINT '>> -------------';


		set @start_time = GETDATE();
		PRINT '>>6 Truncating table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		print '>> Inserting data into table: silver.erp_loc_a101'
		INSERT  into silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
		SELECT 
			id, cat, subcat, maintenance
		FROM bronze.erp_px_cat_g1v2 t 

		set @end_time = GETDATE();
		PRINT '>> Load duration: ' + cast(datediff(second, @start_time,  @end_time) as varchar) + ' seconds';
		PRINT '>> -------------';

		PRINT '==============================================================';
		PRINT 'Loading Silver Layer is completed'
		PRINT ' Batch Load duration: ' + cast(datediff(second, @batch_start_time,  @end_time) as varchar) + ' seconds';
		PRINT '>> -------------';
		PRINT '==============================================================';

	END TRY

	BEGIN CATCH
		PRINT '================================================================'
		PRINT 'Error occurred during Loading Silver Layer'
		PRINT 'Error Message: ' + Error_Message();
		PRINT 'Error number: ' + cast (error_number() as nvarchar);
		PRINT 'Error state: ' + cast (error_state() as nvarchar);
		PRINT '================================================================'

	END CATCH

End
