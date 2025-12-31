/*
==================================================================================================================
User Stored Procedure: Load Silver Table (Bronze -> Silver)
==================================================================================================================
Script Purpose:
	This script loads the silver table [silver.erp_cust_az12]. It performs series of data transformations such
	as data cleansing, enrichment, and standardization.

Parameter: @job_run_id UNIQUEIDENTIFIER = NULL

Usage: EXEC silver.usp_load_silver_erp_cust_az12;

Note:
	* Running this script independently assigns a job_run_id local to this procedure.
	* Ensure to run the master procedure as it performs a full ETL run and assigns similar job_run_id 
	  across tables and layers within the same ETL run, allowing for unified tracking.
==================================================================================================================
*/
CREATE OR ALTER PROCEDURE silver.usp_load_silver_erp_cust_az12 @job_run_id UNIQUEIDENTIFIER = NULL AS
BEGIN
	-- Abort transaction on severe error
	SET XACT_ABORT ON;

	-- Declare and map values to variables where necessary
	DECLARE
	@step_run_id UNIQUEIDENTIFIER = NEWID(),
	@layer NVARCHAR(50) = 'silver',
	@table_name NVARCHAR(50) = 'erp_cust_az12',
	@step_name NVARCHAR(50) = 'usp_load_silver_erp_cust_az12',
	@start_time DATETIME,
	@end_time DATETIME,
	@step_duration INT,
	@step_status NVARCHAR(50) = 'RUNNING',
	@rows_source INT,
	@rows_loaded INT,
	@rows_diff INT,
	@source_path NVARCHAR(50) = 'bronze.erp_cust_az12',
	@pk_nulls INT,
	@pk_duplicates INT;

	-- Map value to job_run_id if NULL
	IF @job_run_id IS NULL SET @job_run_id = NEWID();

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
		TRUNCATE TABLE silver_stg.erp_cust_az12;

		-- Perform data transformations on source table
		WITH data_transformations AS
		(
		SELECT
			CASE 
				WHEN TRIM(cid) LIKE('NAS%') THEN REPLACE(TRIM(cid), 'NAS', '')
				ELSE TRIM(cid)
			END AS cid,
			CASE	
				WHEN bdate > GETDATE() THEN NULL
				ELSE bdate
			END AS bdate,
			CASE 
				WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
				WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
				ELSE 'Unknown'
			END AS gen
		FROM bronze.erp_cust_az12
		)
		, metadata_columns AS
		(
		SELECT
			cid,
			bdate,
			gen,
			@job_run_id AS dwh_job_run_id,
			CONCAT_WS('|',
			cid,
			bdate,
			gen) AS dwh_raw_row,
			HASHBYTES('SHA2_256', CONCAT_WS('|',
			COALESCE(CAST(cid AS VARBINARY(64)), '~'),
			COALESCE(CAST(bdate AS VARBINARY(64)), '~'),
			COALESCE(CAST(gen AS VARBINARY(64)), '~'))) AS dwh_row_hash
		FROM data_transformations
		)
		-- Retrieve newly transformed records and load into corresponding silver staging table
		INSERT INTO silver_stg.erp_cust_az12
		(
			cid,
			bdate,
			gen,
			dwh_job_run_id,
			dwh_raw_row,
			dwh_row_hash
		)
		SELECT
			mc.cid,
			mc.bdate,
			mc.gen,
			mc.dwh_job_run_id,
			mc.dwh_raw_row,
			mc.dwh_row_hash
		FROM metadata_columns mc
		LEFT JOIN silver.erp_cust_az12 sec
		ON mc.cid = sec.cid
		WHERE sec.dwh_row_hash IS NULL;

		-- Retrieve total number of new records from silver_stg
		SELECT @rows_source = COUNT(*) FROM silver_stg.erp_cust_az12;

		-- Error handling if total number of new records is NULL or zero
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
		DROP TABLE IF EXISTS #dq_metrics_erp_cust_az12;

		-- Create and load into temp table vital dq metrics
		SELECT
			COUNT(*) AS rows_checked,
			SUM(CASE WHEN cid IS NULL THEN 1 ELSE 0 END) AS pk_nulls,
			COUNT(cid) - COUNT(DISTINCT cid) AS pk_duplicates,
			SUM(CASE WHEN bdate > GETDATE() THEN 1 ELSE 0 END) AS invalid_bdate,
			SUM(CASE WHEN gen NOT IN ('Male', 'Female', 'Unknown') THEN 1 ELSE 0 END) AS invalid_gen
			INTO #dq_metrics_erp_cust_az12
		FROM silver_stg.erp_cust_az12;

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
		'CRITICAL RULE "pk_nulls" VIOLATED: Unable to load "' + @table_name + '".' ELSE NULL END AS err_detail
		FROM #dq_metrics_erp_cust_az12
		UNION ALL
		SELECT @job_run_id, @step_run_id, @layer, @table_name, 'pk_duplicates', rows_checked, pk_duplicates AS rows_failed,
		CASE WHEN pk_duplicates > 0 THEN 'FAILED' ELSE 'SUCCESS' END AS dq_status, CASE WHEN pk_duplicates > 0 THEN
		'CRITICAL RULE "pk_duplicates" VIOLATED: Unable to load "' + @table_name + '".' ELSE NULL END AS err_detail
		FROM #dq_metrics_erp_cust_az12;

		-- Controlled error handling when critical data quality rule is violated
		IF EXISTS
		(
			SELECT 1 FROM [audit].etl_data_quality dq INNER JOIN [audit].etl_data_quality_control dqc
			ON dq.dq_layer = dqc.dq_layer AND dq.dq_table_name = dqc.dq_table_name AND dq.dq_check_name = dqc.dq_check_name
			WHERE dq.step_run_id = @step_run_id AND dq.dq_status = 'FAILED' AND dqc.stop_on_failure = 1 AND dqc.is_active = 1
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
		END;
		
		-- Begin transaction
		BEGIN TRAN;

		-- Merge into target using source
		MERGE silver.erp_cust_az12 tgt
		USING silver_stg.erp_cust_az12 src
			ON tgt.cid = src.cid
	
		-- Update outdated records
		WHEN MATCHED 
			AND tgt.dwh_row_hash != src.dwh_row_hash THEN
		UPDATE SET
			tgt.bdate = src.bdate,
			tgt.gen = src.gen,
			tgt.dwh_job_run_id = src.dwh_job_run_id,
			tgt.dwh_raw_row = src.dwh_raw_row,
			tgt.dwh_row_hash = src.dwh_row_hash

		-- Insert new records
		WHEN NOT MATCHED BY TARGET THEN
			INSERT
			(
				cid,
				bdate,
				gen,
				dwh_job_run_id,
				dwh_raw_row,
				dwh_row_hash
			)
			VALUES
			(
				src.cid,
				src.bdate,
				src.gen,
				src.dwh_job_run_id,
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
