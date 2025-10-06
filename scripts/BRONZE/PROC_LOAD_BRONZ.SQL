------------------------------------------------------------------------


CREATE OR ALTER PROCEDURE BRONZE.LOAD_BRONZE AS
BEGIN
    DECLARE @START_TIME DATETIME ,@END_TIME DATETIME,@BATCH_START_TIME DATETIME, @BATCH_END_TIME DATETIME;
    BEGIN TRY
    SET @BATCH_START_TIME = GETDATE()
    PRINT'====================================================';
    PRINT'LOADING BRONZE LAYER:';
    PRINT'====================================================';


    PRINT '---------------------------------------------------';
    PRINT'LOADING CRM TABLES:';
    PRINT '---------------------------------------------------';

    SET @START_TIME = GETDATE();
    PRINT 'TRUNCATING TABLE : BRONZE.CRM_CUST_INFO';
    TRUNCATE TABLE BRONZE.CRM_CUST_INFO;
    PRINT '>> INSERTING DATA INTO : BRONZE.CRM_CUST_INFO';
    BULK INSERT BRONZE.CRM_CUST_INFO
    FROM 'D:\Compressed\DWH\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
    WITH(
    FIRSTROW = 2,
    FIELDTERMINATOR =',',
    TABLOCK
    );
    SET @END_TIME = GETDATE();
    PRINT '>> LOAD DURATION :' + CAST( DATEDIFF(SECOND,@START_TIME, @END_TIME) AS NVARCHAR) + 'SECONDS';
    
    --------
    PRINT'--------------------------------------------------';
    SET @START_TIME = GETDATE();
    PRINT 'TRUNCATING TABLE : BRONZE.CRM_PRD_INFO';
    TRUNCATE TABLE BRONZE.CRM_PRD_INFO;
    PRINT '>> INSERTING DATA INTO : BRONZE.CRM_PRD_INFO';
    BULK INSERT BRONZE.CRM_PRD_INFO
    FROM 'D:\Compressed\DWH\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
    WITH(
    FIRSTROW = 2,
    FIELDTERMINATOR =',',
    TABLOCK
    );
    SET @END_TIME = GETDATE();
    PRINT'>> LOAD DURATION :' + CAST( DATEDIFF(SECOND,@START_TIME, @END_TIME) AS NVARCHAR) + 'SECONDS';

    -------
    PRINT'---------------------------------------------------';
    SET @START_TIME = GETDATE();
    PRINT 'TRUNCATING TABLE : BRONZE.CRM_SALES_DETAILS';
    TRUNCATE TABLE BRONZE.CRM_SALES_DETAILS;
    PRINT '>> INSERTING DATA INTO : BRONZE.CRM_SALES_DETAILS';
    BULK INSERT BRONZE.CRM_SALES_DETAILS
    FROM 'D:\Compressed\DWH\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
    WITH(
    FIRSTROW = 2,
    FIELDTERMINATOR =',',
    TABLOCK
    );
    SET @END_TIME = GETDATE();
    PRINT'>> LOAD DURATION :' + CAST( DATEDIFF(SECOND,@START_TIME, @END_TIME) AS NVARCHAR) + 'SECONDS';

    ----------

    PRINT '---------------------------------------------------';
    PRINT'LOADING ERP TABLES:';
    PRINT '---------------------------------------------------';

    PRINT'---------------------------------------------------';
    SET @START_TIME = GETDATE();
    PRINT 'TRUNCATE TABLE : BRONZE.ERP_CUST_AZ12';
    TRUNCATE TABLE BRONZE.ERP_CUST_AZ12;
    PRINT '>> INSERTING DATA INTO : BRONZE.ERP_CUST_AZ12';
    BULK INSERT BRONZE.ERP_CUST_AZ12
    FROM 'D:\Compressed\DWH\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
    WITH(
    FIRSTROW = 2,
    FIELDTERMINATOR =',',
    TABLOCK
    );
    SET @END_TIME = GETDATE();
    PRINT'>> LOAD DURATION :' + CAST( DATEDIFF(SECOND,@START_TIME, @END_TIME) AS NVARCHAR) + 'SECONDS';

    ---------
    PRINT'---------------------------------------------------';
    SET @START_TIME = GETDATE();
    PRINT 'TRUNCATE TABLE : BRONZE.ERP_LOC_A101';
    TRUNCATE TABLE BRONZE.ERP_LOC_A101;
    PRINT'>> INSERTING DATA INTO: BRONZE.ERP_LOC_A101';
    BULK INSERT BRONZE.ERP_LOC_A101
    FROM 'D:\Compressed\DWH\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
    WITH(
    FIRSTROW = 2,
    FIELDTERMINATOR =',',
    TABLOCK
    );
    SET @END_TIME = GETDATE();
    PRINT'>> LOAD DURATION :' + CAST( DATEDIFF(SECOND,@START_TIME, @END_TIME) AS NVARCHAR) + 'SECONDS';
    ----------
    PRINT'---------------------------------------------------';
    SET @START_TIME = GETDATE();
    PRINT 'TRUNCATE TABLE : BRONZE.ERP_PX_CAT_GLV2'
    TRUNCATE TABLE BRONZE.ERP_px_cat_g1v2;
    PRINT '>> INSERTING DATA INTO : BRONZE.ERP_PX_CAT_GLV2'
    BULK INSERT BRONZE.ERP_px_cat_g1v2
    FROM 'D:\Compressed\DWH\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
    WITH(
    FIRSTROW = 2,
    FIELDTERMINATOR =',',
    TABLOCK
    );
    SET @END_TIME = GETDATE();
    PRINT'>> LOAD DURATION :' + CAST( DATEDIFF(SECOND,@START_TIME, @END_TIME) AS NVARCHAR) + 'SECONDS';


    SET @BATCH_END_TIME =GETDATE();
    PRINT '=====================================';
    PRINT 'LOADING BRONZE LAYER IS COMPLETED ';
    PRINT ' TOTAL LOAD DURATION: ' +CAST(DATEDIFF(SECOND,@BATCH_START_TIME, @BATCH_END_TIME) AS NVARCHAR) + 'SECONDS';
    END TRY
    BEGIN CATCH
    PRINT '=============================================='
    PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
    PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
    PRINT 'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR);
    PRINT 'ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR)

    END CATCH

END

-------------------------------------------------------------------------
-------------------------------------------------------------------------
SELECT COUNT(*) FROM BRONZE.CRM_CUST_INFO
select * from bronze.crm_cust_info

SELECT COUNT(*) FROM BRONZE.CRM_PRD_INFO
SELECT * FROM BRONZE.CRM_PRD_INFO

SELECT COUNT(*) FROM BRONZE.CRM_SALES_DETAILS;
SELECT * FROM BRONZE.CRM_SALES_DETAILS;

SELECT COUNT(*) FROM BRONZE.ERP_CUST_AZ12;
SELECT * FROM BRONZE.ERP_CUST_AZ12;

SELECT COUNT(*) FROM BRONZE.ERP_LOC_A101;
SELECT * FROM BRONZE.ERP_LOC_A101;

SELECT COUNT(*) FROM BRONZE.ERP_PX_CAT_G1V2;
SELECT * FROM BRONZE.ERP_PX_CAT_G1V2;

---------------------------------------------------------------
--------------------------------------------------------------
EXEC BRONZE.LOAD_BRONZE
---------------------------------------------------------------
---------------------------------------------------------------
