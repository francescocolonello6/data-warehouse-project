SCOPO: Questa query serve a trasportare i dati dalla fonte fino alle nostre tabelle, si dichiara il percorso del file  e le istruzioni per prenderne il contenuto, come la prima riga da selezionare e il delimitatore.
   Successivamente si utilizza la funzione print per specificare cosa si sta facendo e impostando messaggi di errore (utile per debugging). Infine aggiungono i tempi di processazione per ogni tabella insieme al tempo totale.
  Infine, con questa query, si crea anche una sp.

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @start_load_bronze DATETIME, @end_load_bronze DATETIME;
	BEGIN TRY
		PRINT '===================================================================';
		PRINT 'Loading Bronzr Layer';
		PRINT '===================================================================';

		PRINT '-------------------------------------------------------------------';
		PRINT 'LOADING CRM TABLES';
		PRINT '-------------------------------------------------------------------';

		SET @start_load_bronze = GETDATE();
		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info
		PRINT '>> INSERTING DATA: bronze.crm_cust_info';

		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\Francesco\OneDrive\Desktop\Progetti\01 - Carrer\BECOMING IT\Corso SQL - BAARA\sql-data-warehouse-project-main\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST (DATEDIFF (second, @start_time, @end_time) AS NVARCHAR) + 'seconds'
		PRINT '-------------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info
		PRINT '>> INSERTING DATA: bronze.crm_prd_info';

		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\Francesco\OneDrive\Desktop\Progetti\01 - Carrer\BECOMING IT\Corso SQL - BAARA\sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST (DATEDIFF (second, @start_time, @end_time) AS NVARCHAR) + 'seconds'
		PRINT '-------------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details
		PRINT '>> INSERTING DATA: bronze.crm_sales_details';

		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\Francesco\OneDrive\Desktop\Progetti\01 - Carrer\BECOMING IT\Corso SQL - BAARA\sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST (DATEDIFF (second, @start_time, @end_time) AS NVARCHAR) + 'seconds'
		PRINT '-------------------------------------------------------------------';

		PRINT '-------------------------------------------------------------------';
		PRINT 'LOADING ERP TABLES';
		PRINT '-------------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12
		PRINT '>> INSERTING DATA: bronze.erp_cust_az12';

		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\Francesco\OneDrive\Desktop\Progetti\01 - Carrer\BECOMING IT\Corso SQL - BAARA\sql-data-warehouse-project-main\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST (DATEDIFF (second, @start_time, @end_time) AS NVARCHAR) + 'seconds'
		PRINT '-------------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101
		PRINT '>> INSERTING DATA: bronze.erp_loc_a101';


		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\Francesco\OneDrive\Desktop\Progetti\01 - Carrer\BECOMING IT\Corso SQL - BAARA\sql-data-warehouse-project-main\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST (DATEDIFF (second, @start_time, @end_time) AS NVARCHAR) + 'seconds'
		PRINT '-------------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2
		PRINT '>> INSERTING DATA: bronze.erp_px_cat_g1v2';

		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\Francesco\OneDrive\Desktop\Progetti\01 - Carrer\BECOMING IT\Corso SQL - BAARA\sql-data-warehouse-project-main\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST (DATEDIFF (second, @start_time, @end_time) AS NVARCHAR) + 'seconds'
		PRINT '-------------------------------------------------------------------';
		SET @end_load_bronze = GETDATE();
		PRINT '-------------------------------------------------------------------';
    PRINT ' Il caricamento del layer bronze è concluso';
		PRINT '>> TOTAL LOAD DURATIION: ' + CAST (DATEDIFF (second, @start_load_bronze, @end_load_bronze) AS NVARCHAR) + 'seconds'

	END TRY
	BEGIN CATCH
		PRINT '====================================================';
		PRINT 'ERROR OCCURED WHILE LOADING THE BRONZE LAYER';
		PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
		PRINT 'ERROR MESSAGE' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR MESSAGE' + CAST (ERROR_STATE() AS NVARCHAR);

		PRINT '====================================================';
	END CATCH
END
