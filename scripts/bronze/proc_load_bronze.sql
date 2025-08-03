/* =====================================================================================
  Script: proc_load_bronze.sql
========================================================================================
  Purpose: 
    This stored procedure (bronze.load_bronze) loads data into the Bronze layer of the data warehouse.
    It imports raw data from CSV files into staging tables for both CRM and ERP sources.
    The procedure:
      - Truncates existing data in target Bronze tables.
      - Performs BULK INSERT operations for each source CSV file.
      - Logs execution times for each load step.
      - Handles errors using TRY-CATCH blocks and prints error details.

  Notes:
    - CRM data includes customer info, product info, and sales details.
    - ERP data includes customer, product category, and location information.
--------------------------------------------------------------------------------------------------
*/


CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
 DECLARE @start_time DATETIME, @end_time DATETIME;
 DECLARE @batch_start_time DATETIME, @batch_end_time DATETIME;

 BEGIN TRY
	PRINT '=============================================';
	PRINT 'Loading Bronze Layer';
	PRINT '=============================================';
	PRINT '---------------------------------------------';
	PRINT 'Loading CRM Tables';
	
	SET @batch_start_time = GETDATE();

	
	SET @start_time = GETDATE();
	TRUNCATE TABLE bronze.crm_cust_info;
	BULK INSERT bronze.crm_cust_info
	FROM 'C:\Users\rober\Desktop\SQL_project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
	WITH( 
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
	SET @end_time = GETDATE();
	PRINT '>> Load Duration ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR ) + 'seconds';
	PRINT '>> -------------';
	


	SET @start_time = GETDATE();
	TRUNCATE TABLE bronze.crm_prd_info
	BULK INSERT bronze.crm_prd_info
	FROM 'C:\Users\rober\Desktop\SQL_project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
	WITH(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
	SET @end_time = GETDATE();
	PRINT '>> Load Duration ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR ) + 'seconds';
	PRINT '>> -------------';



	SET @start_time = GETDATE();
	TRUNCATE TABLE bronze.crm_sales_details
	BULK INSERT bronze.crm_sales_details
	FROM 'C:\Users\rober\Desktop\SQL_project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
	WITH(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK

	);
	SET @end_time = GETDATE();
	PRINT '>> Load Duration ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR ) + 'seconds';
	PRINT '>> -------------';



	PRINT '---------------------------------------------';
	PRINT 'Loading ERP Tables';

	SET @start_time = GETDATE();
	TRUNCATE TABLE bronze.erp_cust_az12
	BULK INSERT bronze.erp_cust_az12
	FROM 'C:\Users\rober\Desktop\SQL_project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
	SET @end_time = GETDATE();
	PRINT '>> Load Duration ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR ) + 'seconds';
	PRINT '>> -------------';

	 SET @start_time = GETDATE();
	 TRUNCATE TABLE bronze.erp_px_cat_g1v2
	 BULK INSERT bronze.erp_px_cat_g1v2
	 FROM 'C:\Users\rober\Desktop\SQL_project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
	 WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	 );
	SET @end_time = GETDATE();
	PRINT '>> Load Duration ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR ) + 'seconds';
	PRINT '>> -------------';

	SET @start_time = GETDATE();
	TRUNCATE TABLE bronze.erp_loc_a101
	BULK INSERT bronze.erp_loc_a101
	FROM 'C:\Users\rober\Desktop\SQL_project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
	WITH(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
	SET @end_time = GETDATE();
	PRINT '>> Load Duration' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR ) + 'seconds';
	PRINT '>> -------------';

	SET @batch_end_time = GETDATE();

	PRINT '		-Total load time of the tables: ' + CAST(DATEDIFF(second, @batch_end_time, @batch_end_time) AS NVARCHAR) + ' seconds';

	END TRY
	BEGIN CATCH
	PRINT '=============================================';
	PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
	PRINT 'Error Message' + ERROR_MESSAGE();
	PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
	PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
	PRINT '=============================================';
	END CATCH
 END;
