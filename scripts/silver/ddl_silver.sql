/*
==========================================================================================
DDL Script: Create Silver Tables
==========================================================================================
Script Purpose:
	Create tables in silver schema, dropping existing tables if they exist.

	Run this script to re-define the DDL structure of "Silver" Tables.
==========================================================================================
*/

print 'crm source';

-- 1/6
IF OBJECT_ID('silver.crm_cust_info', 'U') is not null
	DROP table silver.crm_cust_info;

CREATE TABLE silver.crm_cust_info
(
	cst_id int
	,cst_key nvarchar(50)
	,cst_firstname nvarchar(50)
	,cst_lastname nvarchar(50)
	,cst_marital_status varchar(50)
	,cst_gndr varchar(50)
	,cst_create_date date
	,dwh_create_date	datetime2 default getdate()
);  

-- 2/6
IF OBJECT_ID('silver.crm_prd_info', 'U') is not null
	DROP table silver.crm_prd_info;

Create Table silver.crm_prd_info
(
	prd_id		int
	,prd_key	nvarchar(50)
	,prd_nm		nvarchar(50)
	,prd_cost	int
	,prd_line	nvarchar(50)
	,prd_start_dt	datetime
	,prd_end_dt		datetime
	,dwh_create_date	datetime2 default getdate()
);

-- 3/6
IF OBJECT_ID('silver.crm_sales_details', 'U') is not null
	DROP table silver.crm_sales_details;

Create table silver.crm_sales_details
(
	sls_ord_num		nvarchar(50)
	,sls_prd_key	nvarchar(50)
	,sls_cust_id int
	,sls_order_dt	date 
	,sls_ship_dt	date 
	,sls_due_dt		date 
	,sls_sales	int
	,sls_quantity	int
	,sls_price	int
	,dwh_create_date	datetime2 default getdate()
);

print 'ERP source';
-- see ..MyTutorials\BaraaSQL_Course\CourseMaterials\SQL_WarehouseProject\sql-data-warehouse-project\datasets\source_erp

-- 4/6
IF OBJECT_ID('silver.erp_cust_az12', 'U') is not null
	DROP table silver.erp_cust_az12;

Create table silver.erp_cust_az12
(
	cid		varchar(50)
	,bdate	date
	,gen	varchar(50)
	,dwh_create_date	datetime2 default getdate()
);

-- 5/6
IF OBJECT_ID('silver.erp_loc_a101', 'U') is not null
	DROP table silver.erp_loc_a101;

Create table silver.erp_loc_a101
(
	cid		varchar(50)
	,cntry	varchar(50)
	,dwh_create_date	datetime2 default getdate()
);

-- 6/6
IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') is not null
	DROP table silver.erp_px_cat_g1v2;

Create table silver.erp_px_cat_g1v2
(
	id			varchar(50)
	,cat		varchar(50)
	,subcat		varchar(50)
	,maintenance varchar(50)
	,dwh_create_date	datetime2 default getdate()
);


