/*
===============================================================================================================
User Store Procedure: Load Bronze Table (Landing -> Bronze)
===============================================================================================================
Script Purpose:
	This script loads [bronze.crm_cust_info]. It also performs logging operations, by inserting vital
	logging details into log tables [etl_step_run] & [etl_error_log].

Parameter: @job_run_id UNIQUEIDENTIFIER = NULL

Usage: EXEC bronze.usp_load_bronze_crm_cust_info;

Note:
	* Running this script independently, return NULL to job_run_id (parameter) in log tables.
	* Ensure to run the master procedure as it assigns similar job_run_id across all tables & layers
	  within the same ETL run, and thus allowing for easy traceability & debugging.
===============================================================================================================
*/
CREATE OR ALTER PROCEDURE bronze.usp_load_bronze_crm_cust_info @job_run_id UNIQUEIDENTIFIER = NULL AS
BEGIN
	-- Abort transaction on severe errors
	SET XACT_ABORT ON;

	-- Declare and assign values to variables
	DECLARE
	@step_run_id UNIQUEIDENTIFIER = NEWID(),
	@layer NVARCHAR(50) = 'bronze',
	@table_name NVARCHAR(50) = 'crm_cust_info',
	@step_name NVARCHAR(50) = 'usp_load_bronze_crm_cust_info',
	@start_time DATETIME,
	@end_time DATETIME,
	@step_duration INT,
	@step_status NVARCHAR(50) = 'RUNNING',
	@rows_source INT,
	@rows_loaded INT,
	@rows_diff INT,
	@source_path NVARCHAR(50) = 'landing.crm_cust_info';

	-- Capture start_time
	SET @start_time = GETDATE();

	-- Insert into log table
	INSERT INTO [audit].etl_step_run
	(
		step_run_id,
		job_run_id,
		layer,
		table_name,
		step_name,
		start_time,
		step_run_status,
		source_path
	)
	VALUES
	(
		@step_run_id,
		@job_run_id,
		@layer,
		@table_name,
		@step_name,
		@start_time,
		@step_status,
		@source_path
	);

	BEGIN TRY
		-- Retrieve total rows from source table
		SELECT @rows_source = COUNT(*) FROM landing.crm_cust_info;

		-- Stop execution if rows_source is NULL or zero
		IF @rows_source IS NULL OR @rows_source = 0 
		BEGIN
			SET @end_time = GETDATE();
			SET @step_duration = DATEDIFF(second, @start_time, @end_time)
			SET @step_status = 'FAILED';
			SET @rows_source = 0;
			SET @rows_loaded = 0;
			SET @rows_diff = 0;

			UPDATE [audit].etl_step_run
			SET
				end_time = @end_time,
				step_run_duration_seconds = @step_duration,
				step_run_status = @step_status,
				rows_source = @rows_source,
				rows_loaded = @rows_loaded,
				rows_diff = @rows_diff
			WHERE step_run_id = @step_run_id

			RETURN;
		END;

		-- Begin Transaction
		BEGIN TRAN;

		-- Trancate Table
		TRUNCATE TABLE bronze.crm_cust_info;

		-- Load data to table
		INSERT INTO bronze.crm_cust_info
		(
			cst_id,
			cst_key,
			cst_first_name,
			cst_last_name,
			cst_marital_status,
			cst_gndr,
			cst_create_date,
			dwh_step_run_id,
			dwh_raw_row,
			dwh_row_hash
		)
		SELECT
			cst_id,
			cst_key,
			cst_first_name,
			cst_last_name,
			cst_marital_status,
			cst_gndr,
			cst_create_date,
			dwh_step_run_id,
			dwh_raw_row,
			HASHBYTES('SHA2_256', UPPER(CAST(dwh_raw_row AS VARBINARY(MAX)))) AS dwh_row_hash
		FROM
		(
		SELECT
			cst_id,
			cst_key,
			cst_first_name,
			cst_last_name,
			cst_marital_status,
			cst_gndr,
			cst_create_date,
			@step_run_id AS dwh_step_run_id,
			CONCAT_WS('|', 
			COALESCE(CAST(cst_id AS NVARCHAR(50)), '~'),
			COALESCE(TRIM(UPPER(CAST(cst_key AS NVARCHAR(50)))), '~'), 
			COALESCE(TRIM(UPPER(CAST(cst_first_name AS NVARCHAR(50)))), '~'), 
			COALESCE(TRIM(UPPER(CAST(cst_last_name AS NVARCHAR(50)))), '~'),
			COALESCE(TRIM(UPPER(CAST(cst_marital_status AS NVARCHAR(50)))), '~'), 
			COALESCE(TRIM(UPPER(CAST(cst_gndr AS NVARCHAR(50)))), '~'), 
			COALESCE(CONVERT(NVARCHAR(50), cst_create_date, 126), '~')) AS dwh_raw_row
		FROM landing.crm_cust_info
		)SUB;

		-- Retrieve total rows loaded
		SET @rows_loaded = @@ROWCOUNT;

		-- Finalize transaction on success
		COMMIT TRAN;

		-- Map values to variables
		SET @end_time = GETDATE();
		SET @step_duration = DATEDIFF(second, @start_time, @end_time);
		SET @step_status = 'SUCCESS';
		SET @rows_diff = @rows_source - @rows_loaded;

		-- Update data in log table on success
		UPDATE [audit].etl_step_run
		SET
			end_time = @end_time,
			step_run_duration_seconds = @step_duration,
			step_run_status = @step_status,
			rows_source = @rows_source,
			rows_loaded = @rows_loaded,
			rows_diff = @rows_diff
		WHERE step_run_id = @step_run_id;
	END TRY

	BEGIN CATCH
		-- Map values to variables in catch block
		SET @end_time = GETDATE();
		SET @step_duration = DATEDIFF(second, @start_time, @end_time);
		SET @step_status = 'FAILED';

		-- Map 0 to rows source if NULL
		IF @rows_source IS NULL SET @rows_source = 0;

		-- Rollback transaction on error
		IF @@TRANCOUNT > 0 ROLLBACK TRAN;

		-- Map 0 to rows loaded if NULL
		IF @rows_loaded IS NULL SET @rows_loaded = 0;

		-- Calculate row difference
		SET @rows_diff = @rows_source - @rows_loaded;

		-- Update data in log table on failure
		UPDATE [audit].etl_step_run
		SET
			end_time = @end_time,
			step_run_duration_seconds = @step_duration,
			step_run_status = @step_status,
			rows_source = @rows_source,
			rows_loaded = @rows_loaded,
			rows_diff = @rows_diff,
			msg = ERROR_MESSAGE()
		WHERE step_run_id = @step_run_id;

		-- Insert error details in log table
		INSERT INTO [audit].etl_error_log
		(
			job_run_id, 
			step_run_id, 
			err_procedure,
			err_number,
			err_message, 
			err_state, 
			err_line,
			err_severity
		)
		VALUES
		(
			@job_run_id, 
			@step_run_id, 
			@step_name,
			ERROR_NUMBER(),
			ERROR_MESSAGE(), 
			ERROR_STATE(), 
			ERROR_LINE(),
			ERROR_SEVERITY()
		);
	END CATCH;
END;
