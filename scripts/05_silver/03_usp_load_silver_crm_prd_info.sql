/*
==================================================================================================================
User Stored Procedure: Load Silver Table (Bronze -> Silver)
==================================================================================================================
Script Purpose:
	This script loads the silver table [silver.crm_prd_info]. It performs series of data transformations such
	as data cleansing, enrichment, and standardization.

Parameter: @job_run_id UNIQUEIDENTIFIER = NULL

Usage: EXEC silver.usp_load_silver_crm_prd_info;

Note:
	* Running this script independently will assign NULL to the parameter @job_run_id is none is provided.
	* Ensure to run the master procedure as it performs a full ETL run and assigns similar job_run_id 
	  across tables and layers within the same ETL run, allowing for unified tracking.
==================================================================================================================
*/
CREATE OR ALTER PROCEDURE silver.usp_load_silver_crm_prd_info @job_run_id UNIQUEIDENTIFIER = NULL AS
BEGIN
	-- Abort transaction on severe error
	SET XACT_ABORT ON;

	-- Declare and map values to variables where necessary
	DECLARE
	@step_run_id UNIQUEIDENTIFIER = NEWID(),
	@layer NVARCHAR(50) = 'silver',
	@table_name NVARCHAR(50) = 'crm_prd_info',
	@step_name NVARCHAR(50) = 'usp_load_silver_crm_prd_info',
	@start_time DATETIME,
	@end_time DATETIME,
	@step_duration INT,
	@step_status NVARCHAR(50) = 'RUNNING',
	@rows_source INT,
	@rows_loaded INT,
	@rows_diff INT,
	@source_path NVARCHAR(50) = 'bronze.crm_prd_info',
	@pk_nulls INT,
	@pk_duplicates INT;

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
		TRUNCATE TABLE silver_stg.crm_prd_info;

		-- Perform data transformations on source table
		WITH data_transformations AS
		(
		SELECT
			prd_id,
			REPLACE(LEFT(TRIM(prd_key), 5), '-', '_') AS cat_id,
			SUBSTRING(TRIM(prd_key), 7, LEN(prd_key)) AS prd_key,
			TRIM(prd_nm) AS prd_nm,
			prd_cost,
			CASE UPPER(TRIM(prd_line))
				WHEN 'M' THEN 'Mountain'
				WHEN 'R' THEN 'Road'
				WHEN 'S' THEN 'Other Sales'
				WHEN 'T' THEN 'Touring'
				ELSE 'Unknown'
			END AS prd_line,
			prd_start_dt,
			CASE 
				WHEN prd_start_dt > prd_end_dt THEN LEAD(DATEADD(day, -1, prd_start_dt)) OVER(PARTITION BY prd_nm ORDER BY prd_start_dt)
				ELSE prd_end_dt
			END AS prd_end_dt
		FROM bronze.crm_prd_info
		)
		, metadata_columns AS
		(
		SELECT
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt,
			@step_run_id AS dwh_step_run_id,
			CONCAT_WS('|',
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt) AS dwh_raw_row,
			HASHBYTES('SHA2_256', CONCAT_WS('|',
			COALESCE(CAST(prd_id AS VARBINARY(MAX)), '~'),
			COALESCE(CAST(cat_id AS VARBINARY(MAX)), '~'),
			COALESCE(CAST(prd_key AS VARBINARY(MAX)), '~'),
			COALESCE(CAST(prd_nm AS VARBINARY(MAX)), '~'),
			COALESCE(CAST(prd_cost AS VARBINARY(MAX)), '~'),
			COALESCE(CAST(prd_line AS VARBINARY(MAX)), '~'),
			COALESCE(CAST(prd_start_dt AS VARBINARY(MAX)), '~'),
			COALESCE(CAST(prd_end_dt AS VARBINARY(MAX)), '~'))) AS dwh_row_hash
		FROM data_transformations
		)
		-- Retrieve newly transformed records and load into corresponding silver staging table
		INSERT INTO silver_stg.crm_prd_info
		(
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt,
			dwh_step_run_id,
			dwh_raw_row,
			dwh_row_hash
		)
		SELECT
			mc.prd_id,
			mc.cat_id,
			mc.prd_key,
			mc.prd_nm,
			mc.prd_cost,
			mc.prd_line,
			mc.prd_start_dt,
			mc.prd_end_dt,
			mc.dwh_step_run_id,
			mc.dwh_raw_row,
			mc.dwh_row_hash
		FROM metadata_columns AS mc
		LEFT JOIN silver.crm_prd_info scp
		ON mc.prd_id = scp.prd_id AND
		mc.dwh_row_hash = scp.dwh_row_hash
		WHERE scp.dwh_row_hash IS NULL;

		-- Retrieve total number of new records from silver_stg
		SELECT @rows_source = COUNT(*) FROM silver_stg.crm_prd_info;

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
		DROP TABLE IF EXISTS #dq_metrics_crm_prd_info;

		-- Create and load into temp table vital dq metrics
		SELECT
			COUNT(*) AS rows_checked,
			SUM(CASE WHEN prd_id IS NULL THEN 1 ELSE 0 END)  AS pk_nulls,
			COUNT(prd_id) - COUNT(DISTINCT prd_id) AS pk_duplicates,
			SUM(CASE WHEN prd_nm != TRIM(prd_nm) THEN 1 ELSE 0 END) AS prd_nm_untrimmed,
			SUM(CASE WHEN prd_cost <= 0 OR prd_cost IS NULL THEN 1 ELSE 0 END) AS invalid_cost,
			SUM(CASE WHEN prd_line NOT IN('Mountain', 'Road', 'Other Sales', 'Touring', 'Unknown') THEN 1 ELSE 0 END) AS invalid_prd_line,
			SUM(CASE WHEN prd_start_dt IS NULL THEN 1 ELSE 0 END) AS invalid_prd_start_dt,
			SUM(CASE WHEN prd_start_dt > prd_end_dt THEN 1 ELSE 0 END) AS invalid_prd_end_dt
			INTO #dq_metrics_crm_prd_info
		FROM silver_stg.crm_prd_info;

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
		FROM #dq_metrics_crm_prd_info
		UNION ALL
		SELECT @job_run_id, @step_run_id, @layer, @table_name, 'pk_duplicates', rows_checked, pk_duplicates AS rows_failed,
		CASE WHEN pk_duplicates > 0 THEN 'FAILED' ELSE 'SUCCESS' END AS dq_status, CASE WHEN pk_duplicates > 0 THEN
		'CRITCAL RULE "pk_duplicates" VIOLATED: Unable to load "' + @table_name + '".' ELSE NULL END AS err_detail 
		FROM #dq_metrics_crm_prd_info
		UNION ALL
		SELECT @job_run_id, @step_run_id, @layer, @table_name, 'invalid_cost', rows_checked, invalid_cost AS rows_failed,
		CASE WHEN invalid_cost > 0 THEN 'FAILED' ELSE 'SUCCESS' END AS dq_status, CASE WHEN invalid_cost > 0 THEN 
		'WARNING: invalid_cost detected.' ELSE NULL END AS err_detail
		FROM #dq_metrics_crm_prd_info
		UNION ALL
		SELECT @job_run_id, @step_run_id, @layer, @table_name, 'invalid_prd_line', rows_checked, invalid_prd_line AS rows_failed,
		CASE WHEN invalid_prd_line > 0 THEN 'FAILED' ELSE 'SUCCESS' END AS dq_status, CASE WHEN invalid_prd_line > 0 THEN 
		'INFO: invalid_prd_line detected.' ELSE NULL END AS err_detail
		FROM #dq_metrics_crm_prd_info
		UNION ALL
		SELECT @job_run_id, @step_run_id, @layer, @table_name, 'invalid_prd_start_dt', rows_checked, invalid_prd_start_dt AS rows_failed,
		CASE WHEN invalid_prd_start_dt > 0 THEN 'FAILED' ELSE 'SUCCESS' END AS dq_status, CASE WHEN invalid_prd_start_dt > 0 THEN 
		'WARNING: invalid_prd_start_dt detected.' ELSE NULL END AS err_detail 
		FROM #dq_metrics_crm_prd_info
		UNION ALL
		SELECT @job_run_id, @step_run_id, @layer, @table_name, 'invalid_prd_end_dt', rows_checked, invalid_prd_end_dt AS rows_failed,
		CASE WHEN invalid_prd_end_dt > 0 THEN 'FAILED' ELSE 'SUCCESS' END AS dq_status, CASE WHEN invalid_prd_end_dt > 0 THEN 
		'WARNING: invalid_prd_end_dt detected.' ELSE NULL END AS err_detail 
		FROM #dq_metrics_crm_prd_info;

		-- Controlled error handling when critical data quality rule is violated
		IF EXISTS
		(
			SELECT 1 FROM [audit].etl_data_quality dq INNER JOIN [audit].etl_data_quality_control dqc
			ON dq.dq_layer = dqc.dq_layer AND dq.dq_table_name = dqc.dq_table_name AND dq.dq_check_name = dqc.dq_check_name
			WHERE (dq.step_run_id = @step_run_id) AND (dq.dq_status = 'FAILED') AND (dqc.stop_on_failure = 1) AND (dqc.is_active = 1)
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
		MERGE silver.crm_prd_info AS tgt
		USING silver_stg.crm_prd_info AS src
			ON tgt.prd_id = src.prd_id
		
		-- Update outdated records
		WHEN MATCHED 
			AND tgt.dwh_row_hash != src.dwh_row_hash
		THEN UPDATE SET
			tgt.cat_id = src.cat_id,
			tgt.prd_key = src.prd_key,
			tgt.prd_nm = src.prd_nm,
			tgt.prd_cost = src.prd_cost,
			tgt.prd_line = src.prd_line,
			tgt.prd_start_dt = src.prd_start_dt,
			tgt.prd_end_dt = src.prd_end_dt,
			tgt.dwh_step_run_id = src.dwh_step_run_id,
			tgt.dwh_raw_row = src.dwh_raw_row,
			tgt.dwh_row_hash = src.dwh_row_hash

		-- Insert new records
		WHEN NOT MATCHED BY TARGET THEN
			INSERT
			(
				prd_id,
				cat_id,
				prd_key,
				prd_nm,
				prd_cost,
				prd_line,
				prd_start_dt,
				prd_end_dt,
				dwh_step_run_id,
				dwh_raw_row,
				dwh_row_hash
			)
			VALUES
			(
				src.prd_id,
				src.cat_id,
				src.prd_key,
				src.prd_nm,
				src.prd_cost,
				src.prd_line,
				src.prd_start_dt,
				src.prd_end_dt,
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
