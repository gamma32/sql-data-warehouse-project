/*
Create Database and Schemas for  Warehouse Project
	
Script purpose:
	Create a new database names "DataWareHouse" after checking if it already exists.
	If the DB exists, it is dropped and recreated.
	Additionally, three schemas are created as part of the security layers.

Warning:
	Running this script will drop DB 'DataWareHouse' if it is found. 
	That action will permanently delete intended DB.
	Make sure to have backup before running this script.
*/

use MASTER
go;



If exists (select 1 from sys.databases where name = 'DataWareHouse')
begin
	ALTER database DataWareHouse set SINGLE_USER with ROLLBACK IMMEDIATE;
	DROP database DataWareHouse;
end

CREATE database DataWareHouse
go;

-- connect to newly created DB
USE DataWareHouse
go;

-- Create Schemas 
create SCHEMA bronze
go;
create SCHEMA silver
go;
create SCHEMA gold
go;
-- 

