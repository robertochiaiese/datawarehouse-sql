
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	PRINT '====================================';
	PRINT 'LOADING SILVER LAYER';
	PRINT '====================================';

	-- Declare the variables to compute the total time
	DECLARE @start_time DATETIME, @end_time DATETIME;
	DECLARE @start_batch_time DATETIME, @end_batch_time DATETIME;

	BEGIN TRY 

	PRINT '--------------------------------------';
	PRINT 'Loading from CRM';
	PRINT '--------------------------------------'

	SET @start_batch_time = GETDATE();

	-- Loading silver.crm_cust_info
	PRINT '>>Truncating silver.crm_cust_info Table';
	TRUNCATE TABLE silver.crm_cust_info;
	PRINT '>>Loading silver.crm_cust_info Table'
	SET @start_time = GETDATE();
	INSERT INTO silver.crm_cust_info (
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_material_status,
		cst_gndr,
		cst_create_date)
	SELECT 
		cst_id,
		cst_key,
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
		CASE WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
			 WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
			 ELSE 'n/a'
		END AS cst_material_status,
		CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			 ELSE 'n/a'
			 END AS cst_gndr,
		cst_create_date
		FROM (	SELECT *, 
			ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS numb_flags
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
			)t
		WHERE numb_flags = 1;
		SET @end_time = GETDATE();
	PRINT 'Time to load silver.crm_cust_info: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
	PRINT '------------';

	
	-- Loading silver.crm_prd_info
	PRINT '>>Truncating silver.crm_prd_info Table'
	TRUNCATE TABLE silver.crm_prd_info;
	PRINT '>>Loading silver.crm_prd_info Table'
	SET @start_time = GETDATE();
	INSERT INTO silver.crm_prd_info (
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt )
	SELECT 
		prd_id,
		REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
		SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
		prd_nm,
		ISNULL(prd_cost, 0) AS prd_cost,
		CASE UPPER(TRIM(prd_line))
			 WHEN 'M' THEN 'Mountain'
			 WHEN 'R' THEN 'Road'
			 WHEN 'S' THEN  'Other Sales'
			 WHEN 'T' THEN  'Touring'
			 ELSE 'n/a'
		END AS prd_line,
		CAST(prd_start_dt AS DATE) AS prd_start_dt,
		CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
	FROM bronze.crm_prd_info
	SET @end_time = GETDATE()
	PRINT 'Time to load silver.crm_prd_info: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
	PRINT '------------';

	
	-- Loading silver.crm_sales_details
	PRINT '>>Truncating silver.crm_sales_details Table'
	TRUNCATE TABLE silver.crm_sales_details;
	PRINT '>>Loading silver.crm_sales_details Table'
	SET @end_time = GETDATE()
	INSERT INTO silver.crm_sales_details(
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price )
	SELECT 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_order_dt AS VARCHAR)AS DATE)
		END sls_order_dt,
	CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) < 8 THEN NULL
		ELSE CAST(CAST(sls_ship_dt AS VARCHAR)AS DATE)
		END sls_ship_dt,
	CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) < 8 THEN NULL
		ELSE CAST(CAST(sls_due_dt AS VARCHAR)AS DATE)
		END sls_due_dt,
	CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
		THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
		END sls_sales,
		sls_quantity,
	CASE WHEN sls_price IS NULL OR sls_price <= 0
		THEN sls_sales / NULLIF(sls_quantity, 0) 
		ELSE sls_price
		END AS sls_price
	FROM bronze.crm_sales_details;

	SET @end_time = GETDATE()
	PRINT 'Time to load silver.crm_sales_details: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
	PRINT '------------';


	PRINT '--------------------------------------';
	PRINT 'Loading from ERP';
	PRINT '--------------------------------------';


	-- Loading silver.erp_cust_az12
	PRINT '>>Truncating silver.erp_cust_az12 Table'
	TRUNCATE TABLE silver.erp_cust_az12;
	PRINT '>>Loading silver.erp_cust_az12 Table'
	SET @start_time = GETDATE()
	INSERT INTO silver.erp_cust_az12(cid, bdate, gen)
	SELECT 
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		ELSE cid
		END cid,
		CASE WHEN bdate > GETDATE() THEN NULL
		ELSE bdate
		END as bdate,
		CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
			 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
			 ELSE 'n/a'
			END gen
	FROM bronze.erp_cust_az12
	SET @end_time = GETDATE()
	PRINT 'Time to load silver.erp_cust_az12: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
	PRINT '------------';



	-- Loading silver.erp_loc_a101
	PRINT '>>Truncating silver.erp_loc_a101 Table'
	TRUNCATE TABLE silver.erp_loc_a101;
	PRINT '>>Loading silver.erp_loc_a101 Table'
	SET @start_time = GETDATE()
	INSERT INTO silver.erp_loc_a101(cid, cntry)
	select
		REPLACE(cid, '-', '')AS cid,
	CASE WHEN UPPER(TRIM(cntry)) IN ('USA', 'US') THEN 'United States'
		 WHEN UPPER(TRIM(cntry)) = 'UK' THEN ' United Kindom'
		 WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
		 WHEN cntry IS NULL OR cntry = '' THEN 'n/a'
		 ELSE TRIM(cntry) 
		 END cntry
	FROM bronze.erp_loc_a101;
	SET @end_time = GETDATE();
	PRINT 'Time to load silver.erp_loc_a101: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
	PRINT '------------';

	
	-- Loading silver.erp_px_cat_g1v2
	PRINT '>>Truncating silver.erp_px_cat_g1v2 Table'
	TRUNCATE TABLE silver.erp_px_cat_g1v2;
	PRINT '>>Loading silver.erp_px_cat_g1v2 Table'
	SET @start_time = GETDATE();
	INSERT INTO silver.erp_px_cat_g1v2(
		id,
		cat,
		subcat,
		maintenance)
	SELECT 
		id,
		cat,
		subcat,
		maintenance
	FROM bronze.erp_px_cat_g1v2;
	SET @end_time = GETDATE();
	PRINT 'Time to load silver.erp_px_cat_g1v2: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
	PRINT '------------';

	-- Total time to load the tables
	SET @end_batch_time = GETDATE();
	PRINT '>>Total batch time for loading: ' + CAST(DATEDIFF(second, @start_batch_time, @end_batch_time) AS NVARCHAR) + 'seconds'
END TRY

	-- In case an error occures 
BEGIN CATCH
	PRINT '=============================================';
	PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER';
	PRINT 'Error Message' + ERROR_MESSAGE();
	PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
	PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
	PRINT '=============================================';

END CATCH
END


