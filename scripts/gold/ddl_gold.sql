/*
  Script: ddl_gold.sql
  Purpose:
    This script defines views for the Gold layer in the data warehouse.
    It transforms data from the Silver layer into business-ready dimensional and fact tables 
    designed for analytics and reporting use cases.

  Description:
    - Creates a customer dimension view:
        • gold.dim_customers: Enriched customer profile combining CRM, demographic, and geographic data.

  Notes:
    - Surrogate keys are generated using ROW_NUMBER for uniqueness and join efficiency.
    - Gender field is normalized with fallback logic for missing or undefined values.
    - LEFT JOINs are used to enrich the base customer data with auxiliary demographic and location details.
    - View is intended for direct consumption by BI tools or downstream analytical pipelines.
*/



-- Create view: gold.dim_products – product dimension with category and cost attributes
CREATE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key, 
    pn.prd_id AS product_id,
    pn.prd_key AS product_number,
    pn.prd_nm AS product_name,
    pn.cat_id AS category_id,
    pc.cat AS category,
    pc.subcat AS subcategory,
    pc.maintenance,
    pn.prd_cost AS cost,
    pn.prd_line AS product_line,
    pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL;



-- Create view: gold.fact_sales – sales fact table with customer and product references
CREATE VIEW gold.fact_sales AS 
SELECT 
    sd.sls_ord_num AS order_number,
    pr.product_key,
    cu.customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt AS shipping_date,
    sd.sls_due_dt AS due_date,
    sd.sls_sales AS sales_amound,
    sd.sls_quantity AS quantity,
    sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
    ON sd.sls_cust_id = cu.customer_id;


-- Create view: gold.dim_customers – enriched customer dimension for analytics
CREATE VIEW gold.dim_customers AS 
SELECT 
    ROW_NUMBER() OVER (ORDER BY ci.cst_id ASC) AS customer_key,
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    ci.cst_material_status AS material_status,
    CASE 
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr 
        ELSE COALESCE(ca.gen, 'n/a')
    END AS gender,
    ca.bdate AS birthdate,
    la.cntry AS country,
    ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid;
