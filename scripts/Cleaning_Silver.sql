CREATE OR ALTER PROCEDURE silver.Load_silver AS
BEGIN
	PRINT 'SCOPO : Pulire i dati dalle tabelle bronze e inserirli nel silver'

	PRINT 'CRM CUST INFO - INSERIMENTO E PULIZIA DATI'

	PRINT '>> Truncating Table: silver.crm_cust_info'
	TRUNCATE TABLE silver.crm_cust_info
	PRINT '>> Inserting Data Into: silver.crm_cust_info'

	INSERT INTO silver.crm_cust_info (
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date)

	SELECT
	cst_id,

	cst_key,

	TRIM (cst_firstname) as cst_firstname,
	TRIM (cst_lastname) as cst_lastname,
	CASE WHEN UPPER (cst_marital_status) = 'S' THEN 'Single'
		 WHEN UPPER (cst_marital_status) = 'M' THEN 'Married'
		 ELSE 'N/A'
	END cst_marital_status,

	CASE WHEN UPPER (cst_gndr) = 'F' THEN 'Female'
		 WHEN UPPER (cst_gndr) = 'M' THEN 'Male'
		 ELSE 'N/A'
	END cst_gndr,

	cst_create_date
	FROM (
	SELECT
		*,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
	)T where flag_last = 1



	PRINT 'CRM PRD INFO - INSERIMENTO E PULIZIA DATI'

	PRINT '>> Truncating Table: silver.crm_prd_info'
	TRUNCATE TABLE silver.crm_prd_info
	PRINT '>> Inserting Data Into: silver.crm_prd_info'

	INSERT INTO silver.crm_prd_info (
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_id,
	prd_end_dt
	)

		SELECT 
		prd_id,
		REPLACE (SUBSTRING(prd_key, 1, 5), '-','_') as cat_id,
		SUBSTRING (prd_key, 7, len(prd_key)) as prd_key,
		prd_nm,
		ISNULL(prd_cost,0) AS prd_cost,
		CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
			 WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
			 WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
			 WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
		ELSE 'n/a'
		END AS prd_line,
		CAST (prd_start_id as DATE) as prd_start_dt,
		CAST (LEAD(prd_start_id) OVER (partition by prd_key order by prd_start_id)-1 as date) as prd_end_dt
		FROM bronze.crm_prd_info



	PRINT 'CRM SALES DETAILS - INSERIMENTO E PULIZIA DATI'

	PRINT '>> Truncating Table: silver.crm_sales_details'
	TRUNCATE TABLE silver.crm_sales_details
	PRINT '>> Inserting Data Into: silver.crm_sales_details'

	INSERT INTO silver.crm_sales_details (
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
	)

	SELECT
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,

	CASE 
	WHEN sls_order_dt = 0 OR LEN(sls_order_dt)<>8 THEN NULL
	ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
	END as sls_order_dt,

	CASE 
	WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt)<>8 THEN NULL
	ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
	END as sls_ship_dt,

	CASE 
	WHEN sls_due_dt = 0 OR LEN(sls_due_dt)<>8 THEN NULL
	ELSE CAST (CAST(sls_due_dt AS VARCHAR) AS DATE)
	END as sls_due_dt,

	CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales <> ABS(sls_price) * sls_quantity THEN ABS (sls_price) * sls_quantity
		 ELSE sls_sales
	END AS sls_sales,
	sls_quantity,

	CASE WHEN sls_price IS NULL OR sls_price <= 0 THEN ABS(sls_sales) / NULLIF (sls_quantity,0)
		 ELSE sls_price
	END AS sls_price

	FROM bronze.crm_sales_details



	PRINT 'CRM ERP CUST AZ12 - INSERIMENTO E PULIZIA DATI'

	PRINT '>> Truncating Table: silver.erp_cust_az12'
	TRUNCATE TABLE silver.erp_cust_az12
	PRINT '>> Inserting Data Into: silver.erp_cust_az12'

	INSERT INTO silver.erp_cust_az12 (
	cid,
	bdate,
	gen
	)

	SELECT 
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING (cid,4, LEN(cid))
		 ELSE cid
	END cid,

	CASE WHEN bdate > GETDATE() THEN NULL
		 ELSE bdate
	END AS bdate,

	CASE WHEN UPPER(TRIM(gen)) IN ( 'F', 'FEMALE') THEN 'Female'
		 WHEN UPPER(TRIM(gen)) IN ( 'M', 'MALE') THEN 'Male'
		 ELSE 'N/A'
	END AS gen

	FROM bronze.erp_cust_az12

	
	PRINT 'CRM ERP LOC A101 - INSERIMENTO E PULIZIA DATI'

	PRINT '>> Truncating Table: silver.erp_loc_a101'
	TRUNCATE TABLE silver.erp_loc_a101
	PRINT '>> Inserting Data Into: silver.erp_loc_a101'

	INSERT INTO silver.erp_loc_a101(
	cid,
	cntry
	)

	SELECT 
	REPLACE (cid, '-','') cid,
	CASE WHEN TRIM(UPPER(cntry)) = 'DE' THEN 'Germany'
		 WHEN TRIM(UPPER(cntry)) IN ('US' , 'USA') THEN 'United States'
		 WHEN TRIM(UPPER(cntry)) = '' OR cntry IS NULL THEN 'N/A'
		 ELSE TRIM(cntry)
	END AS cntry

	FROM bronze.erp_loc_a101


	PRINT 'CRM ERP PX CAT G1V2 - INSERIMENTO E PULIZIA DATI'

	PRINT '>> Truncating Table: silver.erp_px_cat_g1v2'
	TRUNCATE TABLE silver.erp_px_cat_g1v2
	PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2'

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

	FROM bronze.erp_px_cat_g1v2
END
