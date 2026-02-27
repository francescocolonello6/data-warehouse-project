'
SCOPO: Serve per creare il DATABASE  e i relativi schemi

ATTENZIONE: Usare questo script cancellerà e ricreerà il Database, cancellando tutti i dati. 
Prima di utilizzarlo essere sicuri di avere backup'
--Create Database 'Data Warehouse'
	
	USE master;
	
	CREATE DATABASE DataWarehouse;

	USE DataWarehouse;

	CREATE SCHEMA bronze;
	GO
	CREATE SCHEMA silver;
	GO
	CREATE SCHEMA gold;
	GO
