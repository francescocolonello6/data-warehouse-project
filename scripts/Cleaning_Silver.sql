SCOPO : Pulire i dati dalle tabelle bronze e inserirli nel silver

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

select * from silver.crm_cust_info


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

select *
from silver.crm_prd_info
