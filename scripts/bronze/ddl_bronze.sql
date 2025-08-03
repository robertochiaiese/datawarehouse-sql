/*
  Script: ddl_bronze.sql
  Purpose:
    This script defines and creates the Bronze layer tables used for staging raw data from CRM and ERP sources.
    These tables act as the initial landing zone for untransformed data before processing and loading into higher layers.

  Description:
    - Drops existing tables in the 'bronze' schema if they exist to avoid conflicts.
    - Creates CRM-related staging tables:
        • crm_cust_info: Customer master data from the CRM system.
        • crm_prd_info: Product master data from the CRM system.
        • crm_sales_details: Sales transaction records from the CRM system.
    - Creates ERP-related staging tables:
        • erp_cust_az12: Customer demographic data from the ERP system.
        • erp_loc_a101: Customer location data from the ERP system.
        • erp_px_cat_g1v2: Product category and maintenance data from the ERP system.

  Notes:
    - All tables are created under the 'bronze' schema.
    - Basic data types are used to match the structure of the incoming CSV files.
*/

DROP TABLE IF EXISTS bronze.crm_cust_info
CREATE TABLE bronze.crm_cust_info (
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_material_status NVARCHAR(50),
	cst_gndr NVARCHAR(50),
	cst_create_date DATE
);
GO

DROP TABLE IF EXISTS bronze.crm_prd_info
CREATE TABLE bronze.crm_prd_info (
    prd_id       INT,
    prd_key      NVARCHAR(50),
    prd_nm       NVARCHAR(50),
    prd_cost     INT,
    prd_line     NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt   DATETIME
);
GO

DROP TABLE IF EXISTS bronze.crm_sales_details
CREATE TABLE bronze.crm_sales_details (
    sls_ord_num  NVARCHAR(50),
    sls_prd_key  NVARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt INT,
    sls_ship_dt  INT,
    sls_due_dt   INT,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT
);
GO

DROP TABLE IF EXISTS bronze.erp_cust_az12
CREATE TABLE bronze.erp_cust_az12(
cid NVARCHAR(50),
bdate DATE,
gen NVARCHAR(50)
);

DROP TABLE IF EXISTS bronze.erp_loc_a101
CREATE TABLE bronze.erp_loc_a101(
cid NVARCHAR(50),
cntry NVARCHAR(50)
);

DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2
CREATE TABLE bronze.erp_px_cat_g1v2(
id NVARCHAR(50),
cat NVARCHAR(50),
subcat NVARCHAR(50), 
maintenance NVARCHAR(50)
);
