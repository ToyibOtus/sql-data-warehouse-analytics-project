/*
==================================================================================================================
User Stored Procedure: Load Silver Table (Bronze -> Silver)
==================================================================================================================
Script Purpose:
	This script loads the silver table [silver.crm_cust_info]. It performs series of data transformations such
	as data cleansing, enrichment, and standardization.

Parameter: @job_run_id UNIQUEIDENTIFIER = NULL

Usage: EXEC silver.usp_load_silver_crm_cust_info;

Note:
	* Running this script independently will assign NULL to the parameter @job_run_id is none is provided.
	* Ensure to run the master procedure as it performs a full ETL run and assigns similar job_run_id 
	  across tables and layers within the same ETL run, allowing for unified tracking.
==================================================================================================================
*/
CREATE OR ALTER PROCEDURE silver.usp_load_silver_crm_cust_info @job_run_id UNIQUEIDENTIFIER = NULL AS
BEGIN
	-- Abort transaction on severe error
	SET XACT_ABORT ON;

	-- Declare and map values to variables where necessary
	DECLARE 
	@step_run_id UNIQUEIDENTIFIER = NEWID(),
	@layer NVARCHAR(50) = 'silver',
	@table_name NVARCHAR(50) = 'crm_cust_info',
	@step_name NVARCHAR(50) = 'usp_load_silver_crm_cust_info',
	@start_time DATETIME,
	@end_time DATETIME,
	@step_duration INT,
	@step_status NVARCHAR(50) = 'RUNNING',
	@rows_source INT,
	@rows_loaded INT,
	@rows_diff INT,
	@source_path NVARCHAR(50) = 'bronze.crm_cust_info',
	@pk_nulls INT,
	@pk_duplicates INT;

	DECLARE @merge_stats TABLE (merge_action NVARCHAR(50));

	-- Capture start_time
	SET @start_time = GETDATE();

	-- Load log table
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
		-- Delete data in silver staging table
		TRUNCATE TABLE silver_stg.crm_cust_info;

		-- Perform data transformations on source table
		WITH data_transformations AS
		(
			SELECT
				cst_id,
				TRIM(cst_key) AS cst_key,
				TRIM(cst_first_name) AS cst_first_name,
				TRIM(cst_last_name) AS cst_last_name,
				CASE
					WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
					WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
					ELSE 'Unknown'
				END AS cst_marital_status,
				CASE	
					WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
					WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
					ELSE 'Unknown'
				END AS cst_gndr,
				cst_create_date
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
					ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag
				FROM bronze.crm_cust_info
				WHERE cst_id IS NOT NULL
			)SUB1
			WHERE flag = 1
		)
		, metadata_columns AS
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
				cst_id,
				cst_key,
				cst_first_name,
				cst_last_name,
				cst_marital_status,
				cst_gndr,
				cst_create_date) AS dwh_raw_row,
				HASHBYTES('SHA2_256', CAST(CONCAT_WS('|',
				COALESCE(cst_id, '~'),
				COALESCE(cst_key, '~'),
				COALESCE(cst_first_name, '~'),
				COALESCE(cst_last_name, '~'),
				COALESCE(cst_marital_status, '~'),
				COALESCE(cst_gndr, '~'),
				COALESCE(cst_create_date, '~')) AS VARBINARY(MAX))) AS dwh_row_hash
			FROM data_transformations
		)
		-- Retrieve newly transformed records and load into corresponding silver staging table
		INSERT INTO silver_stg.crm_cust_info
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
			mc.cst_id,
			mc.cst_key,
			mc.cst_first_name,
			mc.cst_last_name,
			mc.cst_marital_status,
			mc.cst_gndr,
			mc.cst_create_date,
			mc.dwh_step_run_id,
			mc.dwh_raw_row,
			mc.dwh_row_hash
		FROM metadata_columns mc
		LEFT JOIN silver.crm_cust_info sci
		ON mc.cst_id = sci.cst_id AND mc.dwh_row_hash = sci.dwh_row_hash
		WHERE sci.dwh_row_hash IS NULL;

		-- Retrieve total number of new records from silver_stg
		SELECT @rows_source = COUNT(*) FROM silver_stg.crm_cust_info;

		-- Error handling when total number of new records is NULL or zero
		IF @rows_source IS NULL OR @rows_source = 0
		BEGIN
			SET @end_time = GETDATE();
			SET @step_duration = DATEDIFF(second, @start_time, @end_time);
			SET @step_status = 'NO OPERATION';
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
				rows_diff = @rows_diff,
				msg = 'No new records from source table "' + @source_path + '".'
			WHERE step_run_id = @step_run_id;

			RETURN;
		END;

		-- Drop temporary table if it exist
		DROP TABLE IF EXISTS #dq_metric_crm_cust_info;

		-- Create and load into temp table vital dq metrics
		SELECT
			COUNT(*) AS rows_checked,
			SUM(CASE WHEN cst_id IS NULL THEN 1 ELSE 0 END) AS pk_nulls,
			COUNT(cst_id) - COUNT(DISTINCT cst_id) AS pk_duplicates,
			SUM(CASE WHEN cst_key <> TRIM(cst_key) THEN 1 ELSE 0 END) AS cst_key_untrimmed,
			SUM(CASE WHEN cst_first_name <> TRIM(cst_first_name) THEN 1 ELSE 0 END) AS cst_first_name_untrimmed,
			SUM(CASE WHEN cst_last_name <> TRIM(cst_last_name) THEN 1 ELSE 0 END) AS cst_last_name_untrimmed,
			SUM(CASE WHEN cst_marital_status NOT IN ('Married', 'Single', 'Unknown') THEN 1 ELSE 0 END) AS invalid_marital_status,
			SUM(CASE WHEN cst_gndr NOT IN ('Male', 'Female', 'Unknown') THEN 1 ELSE 0 END) AS invalid_gndr
			INTO #dq_metric_crm_cust_info
		FROM silver_stg.crm_cust_info;

		-- Insert into log etl_data_quality
		INSERT INTO [audit].etl_data_quality
		(
			job_run_id,
			step_run_id,
			dq_layer,
			dq_table_name,
			dq_check_name,
			rows_checked,
			rows_failed,
			dq_status,
			err_detail
		)
		SELECT @job_run_id, @step_run_id, @layer, @table_name, 'pk_nulls', rows_checked, pk_nulls AS rows_failed, 
		CASE WHEN pk_nulls > 0 THEN 'FAILED' ELSE 'SUCCESS' END AS dq_status, CASE WHEN pk_nulls > 0 THEN 
		'CRITCAL RULE "pk_nulls" VIOLATED: Unable to load "' + @table_name + '".' ELSE NULL END AS err_detail
		FROM #dq_metric_crm_cust_info
		UNION ALL
		SELECT @job_run_id, @step_run_id, @layer, @table_name, 'pk_duplicates', rows_checked, pk_duplicates AS rows_failed, 
		CASE WHEN pk_duplicates > 0 THEN 'FAILED' ELSE 'SUCCESS' END AS dq_status, CASE WHEN pk_duplicates > 0 THEN 
		'CRITCAL RULE "pk_duplicates" VIOLATED: Unable to load "' + @table_name + '".' ELSE NULL END AS err_detail
		FROM #dq_metric_crm_cust_info;

		-- Controlled error handling when critical data quality rule is violated
		IF EXISTS
		(
			SELECT 1 FROM [audit].etl_data_quality dq INNER JOIN [audit].etl_data_quality_control dqc
			ON dq.dq_layer = dqc.dq_layer AND dq.dq_table_name = dqc.dq_table_name AND dq.dq_check_name = dqc.dq_check_name
			WHERE (dq.step_run_id = @step_run_id) AND (dq.dq_check_name = 'pk_nulls' OR dq.dq_check_name = 'pk_duplicates') 
			AND (dq.dq_status = 'FAILED')
		)
		BEGIN
			SET @end_time = GETDATE();
			SET @step_duration = DATEDIFF(second, @start_time, @end_time);
			SET @step_status = 'FAILED';
			IF @rows_source IS NULL SET @rows_source = 0;
			SET @rows_loaded = 0;
			SET @rows_diff = @rows_source - @rows_loaded;
			UPDATE [audit].etl_step_run
			SET
				end_time = @end_time,
				step_run_duration_seconds = @step_duration,
				step_run_status = @step_status,
				rows_source = @rows_source,
				rows_loaded = @rows_loaded,
				rows_diff = @rows_diff,
				msg = 'Critical data quality rule violated. Check log table "etl_data_quality" to know which rule was violated.'
			WHERE step_run_id = @step_run_id;

			RETURN;

		END

		-- Begin transaction
		BEGIN TRAN;
		
		-- Merge into target using source
		MERGE INTO silver.crm_cust_info AS tgt
		USING silver_stg.crm_cust_info AS src
		ON src.cst_id = tgt.cst_id

		-- Update outdated records
		WHEN MATCHED 
			AND src.dwh_row_hash != tgt.dwh_row_hash
		THEN UPDATE SET
			tgt.cst_key = src.cst_key,
			tgt.cst_first_name = src.cst_first_name,
			tgt.cst_last_name = src.cst_last_name,
			tgt.cst_marital_status = src.cst_marital_status,
			tgt.cst_gndr = src.cst_gndr,
			tgt.cst_create_date = src.cst_create_date,
			tgt.dwh_step_run_id = src.dwh_step_run_id,
			tgt.dwh_raw_row = src.dwh_raw_row,
			tgt.dwh_row_hash = src.dwh_row_hash

		-- Insert new records
		WHEN NOT MATCHED BY TARGET
		THEN
			INSERT
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
			VALUES
				(
					src.cst_id,
					src.cst_key,
					src.cst_first_name,
					src.cst_last_name,
					src.cst_marital_status,
					src.cst_gndr,
					src.cst_create_date,
					src.dwh_step_run_id,
					src.dwh_raw_row,
					src.dwh_row_hash
				);

			-- Retrieve total rows loaded
			SET @rows_loaded = @@ROWCOUNT;
		
		-- Finalize transaction on success
		COMMIT TRAN;

		-- Map values to variables
		SET @end_time = GETDATE();
		SET @step_duration = DATEDIFF(second, @start_time, @end_time);
		SET @step_status = 'SUCCESS';
		SET @rows_diff = @rows_source - @rows_loaded;

		-- Update log table etl_step_run on success
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

		-- Map zero to row source if NULL
		IF @rows_source IS NULL SET @rows_source = 0;

		-- Rollback transaction on errror
		IF @@TRANCOUNT > 0 ROLLBACK TRAN;

		-- Map zero to rows loaded if NULL
		IF @rows_loaded IS NULL SET @rows_loaded = 0;

		-- Calculate row difference
		SET @rows_diff = @rows_source - @rows_loaded;

		-- Update etl_step_run on failure
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

		-- Load error details into log table
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
