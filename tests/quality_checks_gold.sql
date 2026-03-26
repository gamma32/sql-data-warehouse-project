
/*
================================================================================================
Quality Checks
================================================================================================
Script purpose:
	Perform quality checks to validate integrity, consistency, and accuracy of Gold Layer.
	This script ensure:
	- Uniqueness of surrogate keys in dimension tables(views).
	- Referential integrity between fact and dimension tables.
	- Validation of relationships in the data model for analytical purposes.

Usage Notes:
	- Run these checks after data loading Silver Layer.
	- Investigate and resolve any discrepancies found during checks.

================================================================================================
*/




--===================================================================================
-- Checking 'gold.fact_customers'
--===================================================================================
-- Check for uniqueness of Customer Key in gold.dim_products	

	-- Expectation: no results
	select customer_key, count(*) 'duplicate_count'
	from gold.dim_customers
	group by customer_key
	having count(*) > 1;


--===================================================================================
-- Checking 'gold.dim_products'
--===================================================================================
-- Check for uniqueness of Product Key in gold.dim_products	

	-- Expectation: no results
	select product_key, count(*) 'duplicate_count'
	from gold.dim_products
	group by product_key
	having count(*) > 1;



--===================================================================================
-- Checking 'gold.fact_sales'
--===================================================================================
-- Check the data model connectivity between Fact and Dimensions

	-- Expectation: no results
	select f.*, c.* , p.*
	from gold.fact_sales f
		left join gold.dim_customers c on f.customer_key = c.customer_key
		left join gold.dim_products p  on f.product_key = p.product_key
	where c.customer_id is null	
		or p.product_key is null ;



