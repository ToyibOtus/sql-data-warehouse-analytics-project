/*
=============================================================================
Master Procedure: Perform Full ETL Run
=============================================================================
Script Purpose:
	This script performs a full ETL run. It assigns similar job_run_id
	across all layers, allowing for easy traceability and debugging.
=============================================================================
*/
CREATE OR ALTER PROCEDURE dbo.usp_run_pipeline_dwh AS
BEGIN
-- Abort on severe error
SET XACT_ABORT ON;
	
	-- Declare and map values to variables where necessary
	DECLARE 
	@job_run_id UNIQUEIDENTIFIER = NEWID(),
	@pipeline_name NVARCHAR(50) = 'usp_run_pipeline_dwh',
	@start_time DATETIME,
	@end_time DATETIME,
	@job_duration INT,
	@job_status NVARCHAR(50) = 'RUNNING',
	@trigger_type NVARCHAR(50) = 'MANUAL';

	-- Capture start time
	SET @start_time = GETDATE();

	-- Load log table 
	INSERT INTO [audit].etl_job_run
	(
		job_run_id,
		pipeline_name,
		start_time,
		job_run_status,
		trigger_type
	)
	VALUES
	(
		@job_run_id,
		@pipeline_name,
		@start_time,
		@job_status,
		@trigger_type
	);

	BEGIN TRY
		-- Load landing layer
		EXEC landing.usp_load_landing_crm_cust_info @job_run_id;
		EXEC landing.usp_load_landing_crm_prd_info @job_run_id;
		EXEC landing.usp_load_landing_crm_sales_details @job_run_id;
		EXEC landing.usp_load_landing_erp_cust_az12 @job_run_id;
		EXEC landing.usp_load_landing_erp_loc_a101 @job_run_id;
		EXEC landing.usp_load_landing_erp_px_cat_g1v2 @job_run_id;

		-- Load bronze layer
		EXEC bronze.usp_load_bronze_crm_cust_info @job_run_id;
		EXEC bronze.usp_load_bronze_crm_prd_info @job_run_id;
		EXEC bronze.usp_load_bronze_crm_sales_details @job_run_id;
		EXEC bronze.usp_load_bronze_erp_cust_az12 @job_run_id;
		EXEC bronze.usp_load_bronze_erp_loc_a101 @job_run_id;
		EXEC bronze.usp_load_bronze_erp_px_cat_g1v2 @job_run_id;

		-- Load silver layer
		EXEC silver.usp_load_silver_crm_cust_info @job_run_id;
		EXEC silver.usp_load_silver_crm_prd_info @job_run_id;
		EXEC silver.usp_load_silver_crm_sales_details @job_run_id;
		EXEC silver.usp_load_silver_erp_cust_az12 @job_run_id;
		EXEC silver.usp_load_silver_erp_loc_a101 @job_run_id;
		EXEC silver.usp_load_silver_erp_px_cat_g1v2 @job_run_id;

		-- Load gold layer
		EXEC gold.usp_load_gold_dim_customers @job_run_id;
		EXEC gold.usp_load_gold_dim_products @job_run_id;
		EXEC gold.usp_load_gold_fact_sales @job_run_id;

		-- Map values to variables
		SET @end_time = GETDATE();
		SET @job_duration = DATEDIFF(second, @start_time, @end_time);
		SET @job_status = 'SUCCESS';

		-- Update log table on success
		UPDATE [audit].etl_job_run
			SET
				end_time = @end_time,
				job_run_duration_seconds = @job_duration,
				job_run_status = @job_status
			WHERE job_run_id = @job_run_id;
	END TRY

	BEGIN CATCH
		-- Map values to variables in catch block
		SET @end_time = GETDATE();
		SET @job_duration = DATEDIFF(second, @start_time, @end_time);
		SET @job_status = 'FAILED';

		-- Update log table on failure
		UPDATE [audit].etl_job_run
			SET
				end_time = @end_time,
				job_run_duration_seconds = @job_duration,
				job_run_status = @job_status,
				err_message = ERROR_MESSAGE()
			WHERE job_run_id = @job_run_id;
	END CATCH;
END;
