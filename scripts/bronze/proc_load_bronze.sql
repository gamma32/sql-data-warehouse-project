/*
=============================================================================================
Store Procedure: Load Bronze Layer [ source -> bronze]
=============================================================================================
Script Purpose:
	This sp loads data into the "Bronze" schema from external CSV files.
	It performs the following actions:
	- Truncates bronze tables before loading
	- Uses "Bulk Insert" command to load from CSV files to corresponding tables.

Parameters:
	None. This sp does not take any parameters or return any values or objects.

Usage example:
	
	EXEC bornze.load_bronze ;

=============================================================================================
*/

CREATE or ALTER Procedure  bronze.load_bronze AS

BEGIN 
	Declare @start_time datetime, @end_time datetime, @batch_start_time datetime
	
	Begin TRY
		PRINT '----------------------------------------------------------------------------';
		PRINT 'Loading Bronze Layer...';
		PRINT '----------------------------------------------------------------------------';

		PRINT '-----------------------------------------------------------------';
		PRINT 'Loading CRM tables';
		PRINT '-----------------------------------------------------------------';

		set @batch_start_time=GETDATE()

		set @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		-- 1/6
		Truncate TABLE bronze.crm_cust_info;

		PRINT '>> Insert data into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info  
		FROM 'C:\Users\marco\Documents\MyTutorials\BaraaSQL_Course\CourseMaterials\SQL_WarehouseProject\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		With ( firstrow=2,  fieldTerminator=',' , tablock ); 

		set @end_time = GETDATE();
		PRINT '>> Load duration: ' + cast(datediff(second, @start_time,  @end_time) as varchar) + ' seconds';
		PRINT '>> -------------';

		set @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.[crm_prd_info]';
		-- 2/6
		Truncate table [bronze].[crm_prd_info];

		PRINT '>> Insert data into: bronze.[crm_prd_info]';
		BULK INSERT [bronze].[crm_prd_info]
		FROM 'C:\Users\marco\Documents\MyTutorials\BaraaSQL_Course\CourseMaterials\SQL_WarehouseProject\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		With ( firstrow=2,  fieldTerminator=',' , tablock ); 

		set @end_time = GETDATE();
		PRINT '>> Load duration: ' + cast(datediff(second, @start_time,  @end_time) as varchar) + ' seconds';
		PRINT '>> -------------';

		set @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.[crm_sales_details]';
		-- 3/6
		Truncate table [bronze].[crm_sales_details]

		PRINT '>> Insert data into: bronze.[crm_sales_details]';
		BULK INSERT [bronze].[crm_sales_details]
		FROM 'C:\Users\marco\Documents\MyTutorials\BaraaSQL_Course\CourseMaterials\SQL_WarehouseProject\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		With ( firstrow=2,  fieldTerminator=',' , tablock ); 

		set @end_time = GETDATE();
		PRINT '>> Load duration: ' + cast(datediff(second, @start_time,  @end_time) as varchar) + ' seconds';
		PRINT '>> -------------';


		PRINT '-----------------------------------------------------------------';
		PRINT 'Loading ERP tables';
		PRINT '-----------------------------------------------------------------';

		set @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.[crm_sales_details]';
		-- 4/6
		Truncate table [bronze].[erp_cust_az12]

		PRINT '>> Insert data into: bronze.[erp_cust_az12]';
		BULK INSERT [bronze].[erp_cust_az12]
		FROM 'C:\Users\marco\Documents\MyTutorials\BaraaSQL_Course\CourseMaterials\SQL_WarehouseProject\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		With ( firstrow=2,  fieldTerminator=',' , tablock ); 

		set @end_time = GETDATE();
		PRINT '>> Load duration: ' + cast(datediff(second, @start_time,  @end_time) as varchar) + ' seconds';
		PRINT '>> -------------';

		set @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.[crm_sales_details]';
		-- 5/6
		truncate table [bronze].[erp_loc_a101]

		PRINT '>> Insert data into: bronze.[erp_loc_a101]';
		BULK INSERT [bronze].[erp_loc_a101]
		FROM 'C:\Users\marco\Documents\MyTutorials\BaraaSQL_Course\CourseMaterials\SQL_WarehouseProject\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		With ( firstrow=2,  fieldTerminator=',' , tablock ); 

		set @end_time = GETDATE();
		PRINT '>> Load duration: ' + cast(datediff(second, @start_time,  @end_time) as varchar) + ' seconds';
		PRINT '>> -------------';

		set @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.[crm_sales_details]';
		-- 6/6
		Truncate table [bronze].[erp_px_cat_g1v2]

		PRINT '>> Insert data into: bronze.[erp_px_cat_g1v2]';
		BULK INSERT [bronze].[erp_px_cat_g1v2]
		FROM 'C:\Users\marco\Documents\MyTutorials\BaraaSQL_Course\CourseMaterials\SQL_WarehouseProject\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		With ( firstrow=2,  fieldTerminator=',' , tablock ); 

		set @end_time = GETDATE();
		PRINT '>> Load duration: ' + cast(datediff(second, @start_time,  @end_time) as varchar) + ' seconds';
		PRINT '>> -------------';

		PRINT '==============================================================';
		PRINT 'Loading Bronze Layer is completed'
		PRINT ' Batch Load duration: ' + cast(datediff(second, @batch_start_time,  @end_time) as varchar) + ' seconds';
		PRINT '>> -------------';
		PRINT '==============================================================';


	END TRY

	BEGIN CATCH
		PRINT '================================================================'
		PRINT 'Error occurred during Loading Bronze Layer'
		PRINT 'Error Message: ' + Error_Message();
		PRINT 'Error number: ' + cast (error_number() as nvarchar);
		PRINT 'Error state: ' + cast (error_state() as nvarchar);
		PRINT '================================================================'

	END CATCH

END
