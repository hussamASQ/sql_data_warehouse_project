/*
************************************************
DDL Scripts: Create Gold Views
*************************************************
script purpose:
this script create views for the gold layer in the data warehouse
this gold layer represents the final dimension and fact tables (star schema)

each view performs transformaions and combines data from the silver layer
to produce a clean , enriched, and business_ready dataset.

usage:
-these views can be quaried directly for analytics and reporting
**********************************************************************8
*/

-- =================================================================================
--create dimension: gold.dim_customers
-- ===================================================================================
--=============================================================
-- CREATE DIMENSION: GOLD.DIM-CUSTOMERS
--=============================================================
IF OBJECT_ID('GOLD.DIM_CUSTOMERS','V') IS NOT NULL
DROP VIEW GOLD.DIM_CUSTOMERS;
GO
CREATE VIEW GOLD.DIM_CUSTOMERS AS
SELECT 
ROW_NUMBER() OVER(ORDER BY CST_ID) AS CUSTOMER_KEY,
CI.CST_ID AS COSTUMER_ID,
CI.CST_KEY AS CUSTOMER_NUMBER,
CI.CST_FIRSTNAME AS FIRST_NAME,
CI.CST_LASTNAME AS LAST_NAME,
LA.CNTRY AS COUNTRY,
CI.CST_MARITAL_STATUS AS MARITAL_STATUS,
CASE WHEN CI.CST_GNDR != 'N/A' THEN CI.CST_GNDR --CRM IS THE MASTER FOR GENDER
     ELSE COALESCE(CA.GEN, 'N/A')
END AS GENDER,
CA.BDATE AS BIRTH_DATE,
CI.CST_CREATE_DATE AS CREATE_DATE
FROM SILVER.CRM_CUST_INFO CI
LEFT JOIN SILVER.ERP_CUST_AZ12 CA
ON CI.CST_KEY = CA.CID
LEFT JOIN SILVER.ERP_LOC_A101 LA
ON CI.CST_KEY = LA.CID
---------------------------------------------------------------------------------------
--============================================================
--CREATE DIMENSION: GOLD.DIM_PRODUCTS
--============================================================
IF OBJECT_ID ('GOLD.DIM_PRODUCTS','V') IS NOT NULL
DROP VIEW GOLD.DIM_PRODUCTS;
GO
CREATE VIEW GOLD.DIM_PRODUCTS AS
SELECT 
ROW_NUMBER() OVER(ORDER BY PN.PRD_START_DT, PN.PRD_KEY) AS PRODUCT_KEY,
PN.PRD_ID AS PRODUCT_ID,
PN.PRD_KEY AS PRODUCT_NUMBER,
PN.PRD_NM AS PRODUCT_NAME,
PN.CAT_ID AS CATEGORY_ID,
PC.CAT AS CATEGORY,
PC.SUBCAT AS SUBCATEGORY,
PC.MAINTENANCE,
PN.PRD_COST AS COST,
PN.PRD_LINE AS PRODUCT_LINE,
PN.PRD_START_DT AS START_DATE
FROM SILVER.CRM_PRD_INFO PN
LEFT JOIN SILVER.ERP_PX_CAT_G1V2 PC
ON PN.CAT_ID = PC.ID
WHERE PRD_END_DT IS NULL --FILTERING OUT ALL HISTORICAL DATA
GO
----------------------------------------------------------------------------------------------
--==========================================================================
-- CREATE FACT TABLE: GOLD.FACT_SALES
--==========================================================================
-----------------------------------------------------------------------------
if object_id ('gold.fact_sales', 'V') is not null
   DROP VIEW GOLD.FACT_SALES;

CREATE VIEW GOLD.FACT_SALES AS
SELECT  SD.SLS_ORD_NUM AS ORDER_NUMBER,
        PR.PRODUCT_ID ,
        CU.CUSTOMER_KEY,
        SD.SLS_ORDER_DT AS ORDER_DATE,
        SD.SLS_SHIP_DT AS SHIPPING_DATE,
        SD.SLS_DUE_DT AS DUE_DATE,
        SD.SLS_SALES AS SALES_AMOUNT,
        SD.SLS_QUANTITY AS QUANTITY,
        SD.SLS_PRICE AS PRICE,
        DW_CREATE_DATE
  FROM SILVER.CRM_SALES_DETAILS SD
  LEFT JOIN GOLD.DIM_PRODUCTS PR
  ON SD.SLS_PRD_KEY = PR.PRODUCT_NUMBER
  LEFT JOIN GOLD.DIM_CUSTOMERS CU
  ON SD.SLS_CUST_ID = CU.COSTUMER_ID
  GO
