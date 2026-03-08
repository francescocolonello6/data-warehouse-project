-- CREAZIONI VISTE PER LIVELLO GOLD

-- Questa query prende i dati da:
--   * silver.crm_cust_info
--   * silver.erp_cust_az12
--   * silver.erp_loc_a101
-- Successivamente li aggrega e crea una una vista della tabella gold, qui non ci sono batch, ma solo aggregazioni e data integration dalle silver:

CREATE VIEW gold.dim_customer AS
SELECT
	ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
	ci.cst_id as customer_id,
	ci.cst_key as customer_number,
	ci.cst_firstname as first_name,
	ci.cst_lastname as last_name,
	la.cntry as country,
	ci.cst_marital_status as marital_status,

	CASE WHEN ci.cst_gndr <> 'N/A' THEN  ci.cst_gndr --CRM È IL MASTER PER QUESTI DATI--
		ELSE COALESCE (ca.gen, 'N/A')
	END AS gender,
	ca.bdate as birthdate,
	ci.cst_create_date as create_date


FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON		ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON		ci.cst_key = la.cid

-- Questa query prende i dati da:
--   * 
--   * 
--   * 
-- Successivamente li aggrega e crea una una vista della tabella gold, qui non ci sono batch, ma solo aggregazioni e data integration dalle silver:
