/*
==================================================================================================================
User Stored Procedure: Load Silver Table (Bronze -> Silver)
==================================================================================================================
Script Purpose:
	This script loads the silver table [silver.crm_sales_details]. It performs series of data transformations such
	as data cleansing, enrichment, and standardization.

Parameter: @job_run_id UNIQUEIDENTIFIER = NULL

Usage: EXEC silver.usp_load_silver_crm_sales_details;

Note:
	* Running this script independently will assign NULL to the parameter @job_run_id is none is provided.
	* Ensure to run the master procedure as it performs a full ETL run and assigns similar job_run_id 
	  across tables and layers within the same ETL run, allowing for unified tracking.
==================================================================================================================
*/
CREATE OR ALTER PROCEDURE silver.usp_load_silver_crm_sales_details @job_run_id UNIQUEIDENTIFIER = NULL AS
BEGIN
	-- Abort transaction on severe error
	SET XACT_ABORT ON;

	-- Declare and map values to variables where necessary
	DECLARE
	@step_run_id UNIQUEIDENTIFIER = NEWID(),
	@layer NVARCHAR(50) = 'silver',
	@table_name NVARCHAR(50) = 'crm_sales_details',
	@step_name NVARCHAR(50) = 'usp_load_silver_crm_sales_details',
	@start_time DATETIME,
	@end_time DATETIME,
	@step_duration INT,
	@step_status NVARCHAR(50) = 'RUNNING',
	@rows_source INT,
	@rows_loaded INT,
	@rows_diff INT,
	@source_path NVARCHAR(50) = 'bronze.crm_sales_details',
	@sls_prd_key_nulls INT,
	@sls_cust_id_nulls INT,
	@invalid_sales INT,
	@invalid_quantity INT,
	@invalid_price INT;

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
		TRUNCATE TABLE silver_stg.crm_sales_details;

		-- Perform data transformations on source table
		WITH data_transformations AS
		(
		SELECT
			TRIM(sls_ord_num) AS sls_ord_num,
			TRIM(sls_prd_key) AS sls_prd_key,
			sls_cust_id,
			CASE
				WHEN LEN(sls_order_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS NVARCHAR(10)) AS DATE)
			END AS sls_order_dt,
			CASE
				WHEN LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS NVARCHAR(10)) AS DATE)
			END AS sls_ship_dt,
			CASE
				WHEN LEN(sls_due_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS NVARCHAR(10)) AS DATE)
			END AS sls_due_dt,
			CASE	
				WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * sls_price THEN ABS(sls_quantity * sls_price)
				ELSE sls_sales
			END AS sls_sales,
			sls_quantity,
			CASE
				WHEN sls_price IS NULL OR sls_price = 0 THEN ABS(sls_sales/NULLIF(sls_quantity, 0))
				WHEN sls_price < 0 THEN ABS(sls_price)
				ELSE sls_price
			END AS sls_price
		FROM bronze.crm_sales_details
		)
		, metadata_columns AS
		(
		SELECT
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price,
			@step_run_id AS dwh_step_run_id,
			CONCAT_WS('|',
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price) AS dwh_raw_row,
			HASHBYTES('SHA2_256', 
			CAST(CONCAT_WS('|',
			COALESCE(CAST(sls_ord_num AS VARBINARY(MAX)), '~'),
			COALESCE(CAST(sls_prd_key AS VARBINARY(MAX)), '~'),
			COALESCE(CAST(sls_cust_id AS VARBINARY(MAX)), '~'),
			COALESCE(CAST(sls_order_dt AS VARBINARY(MAX)), '~'),
			COALESCE(CAST(sls_ship_dt AS VARBINARY(MAX)), '~'),
			COALESCE(CAST(sls_due_dt AS VARBINARY(MAX)), '~'),
			COALESCE(CAST(sls_sales AS VARBINARY(MAX)), '~'),
			COALESCE(CAST(sls_quantity AS VARBINARY(MAX)), '~'),
			COALESCE(CAST(sls_price AS VARBINARY(MAX)), '~')) AS VARBINARY(MAX))) AS dwh_row_hash
		FROM data_transformations
		)
		-- Retrieve newly transformed records and load into corresponding silver staging table
		INSERT INTO silver_stg.crm_sales_details
		(
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price,
			dwh_step_run_id,
			dwh_raw_row,
			dwh_row_hash
		)
		SELECT
			mc.sls_ord_num,
			mc.sls_prd_key,
			mc.sls_cust_id,
			mc.sls_order_dt,
			mc.sls_ship_dt,
			mc.sls_due_dt,
			mc.sls_sales,
			mc.sls_quantity,
			mc.sls_price,
			mc.dwh_step_run_id,
			mc.dwh_raw_row,
			mc.dwh_row_hash
		FROM metadata_columns mc
		LEFT JOIN silver.crm_sales_details scs
		ON mc.sls_ord_num = scs.sls_ord_num
		AND mc.sls_prd_key = scs.sls_prd_key
		AND mc.dwh_row_hash = scs.dwh_row_hash
		WHERE scs.dwh_row_hash IS NULL;

		-- Retrieve total number of new records from silver_stg
		SELECT @rows_source = COUNT(*) FROM silver_stg.crm_sales_details;

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
		DROP TABLE IF EXISTS #dq_metrics_crm_sales_details;
		SELECT
			COUNT(*) AS rows_checked,
			SUM(CASE WHEN sls_ord_num != TRIM(sls_ord_num) THEN 1 ELSE 0 END) AS sls_ord_untrimmed,
			SUM(CASE WHEN sls_prd_key != TRIM(sls_prd_key) THEN 1 ELSE 0 END) AS sls_prd_key_untrimmed,
			SUM(CASE WHEN sls_prd_key IS NULL THEN 1 ELSE 0 END) AS sls_prd_key_nulls,
			SUM(CASE WHEN sls_cust_id IS NULL THEN 1 ELSE 0 END) AS sls_cust_id_nulls,
			SUM(CASE WHEN sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt THEN 1 ELSE 0 END) AS invalid_order_dt,
			SUM(CASE WHEN sls_ship_dt > sls_due_dt THEN 1 ELSE 0 END) AS invalid_ship_dt,
			SUM(CASE WHEN sls_due_dt < sls_order_dt OR sls_due_dt < sls_ship_dt THEN 1 ELSE 0 END) AS invalid_due_dt,
			SUM(CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_price * sls_quantity THEN 1 ELSE 0 END) AS invalid_sales,
			SUM(CASE WHEN sls_quantity IS NULL OR sls_quantity <= 0 OR sls_quantity != sls_sales/NULLIF(sls_price, 0) THEN 1 ELSE 0 END) AS invalid_quantity,
			SUM(CASE WHEN sls_price IS NULL OR sls_price <= 0 OR sls_price != sls_sales/NULLIF(sls_quantity, 0) THEN 1 ELSE 0 END) AS invalid_price
			INTO #dq_metrics_crm_sales_details
		FROM silver_stg.crm_sales_details;

		-- Create and load into temp table vital dq metrics
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
		SELECT @job_run_id, @step_run_id, @layer, @table_name, 'sls_prd_key_nulls', rows_checked, sls_prd_key_nulls AS rows_failed,
		CASE WHEN sls_prd_key_nulls > 0 THEN 'FAILED' ELSE 'SUCCESS' END AS dq_status, CASE WHEN sls_prd_key_nulls > 0 THEN
		'CRITICAL RULE "sls_prd_key_nulls" VIOLATED: Unable to load "' + @table_name + '".' ELSE NULL END AS err_detail
		FROM #dq_metrics_crm_sales_details
		UNION ALL
		SELECT @job_run_id, @step_run_id, @layer, @table_name, 'sls_cust_id_nulls', rows_checked, sls_cust_id_nulls AS rows_failed,
		CASE WHEN sls_cust_id_nulls > 0 THEN 'FAILED' ELSE 'SUCCESS' END AS dq_status, CASE WHEN sls_cust_id_nulls > 0 THEN
		'CRITICAL RULE "sls_cust_id_nulls" VIOLATED: Unable to load "' + @table_name + '".' ELSE NULL END AS err_detail 
		FROM #dq_metrics_crm_sales_details
		UNION ALL
		SELECT @job_run_id, @step_run_id, @layer, @table_name, 'invalid_order_dt', rows_checked, invalid_order_dt AS rows_failed,
		CASE WHEN invalid_order_dt > 0 THEN 'FAILED' ELSE 'SUCCESS' END AS dq_status, CASE WHEN invalid_order_dt > 0 THEN
		'WARNING: invalid_order_dt detected' ELSE NULL END AS err_detail 
		FROM #dq_metrics_crm_sales_details
		UNION ALL
		SELECT @job_run_id, @step_run_id, @layer, @table_name, 'invalid_ship_dt', rows_checked, invalid_ship_dt AS rows_failed,
		CASE WHEN invalid_ship_dt > 0 THEN 'FAILED' ELSE 'SUCCESS' END AS dq_status, CASE WHEN invalid_ship_dt > 0 THEN
		'WARNING: invalid_ship_dt detected' ELSE NULL END AS err_detail 
		FROM #dq_metrics_crm_sales_details
		UNION ALL
		SELECT @job_run_id, @step_run_id, @layer, @table_name, 'invalid_due_dt', rows_checked, invalid_due_dt AS rows_failed,
		CASE WHEN invalid_due_dt > 0 THEN 'FAILED' ELSE 'SUCCESS' END AS dq_status, CASE WHEN invalid_due_dt > 0 THEN
		'WARNING: invalid_due_dt detected' ELSE NULL END AS err_detail 
		FROM #dq_metrics_crm_sales_details
		UNION ALL
		SELECT @job_run_id, @step_run_id, @layer, @table_name, 'invalid_sales', rows_checked, invalid_sales AS rows_failed,
		CASE WHEN invalid_sales > 0 THEN 'FAILED' ELSE 'SUCCESS' END AS dq_status, CASE WHEN invalid_sales > 0 THEN
		'CRITICAL RULE "invalid_sales" VIOLATED: Unable to load "' + @table_name + '".' ELSE NULL END AS err_detail 
		FROM #dq_metrics_crm_sales_details
		UNION ALL
		SELECT @job_run_id, @step_run_id, @layer, @table_name, 'invalid_quantity', rows_checked, invalid_quantity AS rows_failed,
		CASE WHEN invalid_quantity > 0 THEN 'FAILED' ELSE 'SUCCESS' END AS dq_status, CASE WHEN invalid_quantity > 0 THEN
		'CRITICAL RULE "invalid_quantity" VIOLATED: Unable to load "' + @table_name + '".' ELSE NULL END AS err_detail 
		FROM #dq_metrics_crm_sales_details
		UNION ALL
		SELECT @job_run_id, @step_run_id, @layer, @table_name, 'invalid_price', rows_checked, invalid_price AS rows_failed,
		CASE WHEN invalid_price > 0 THEN 'FAILED' ELSE 'SUCCESS' END AS dq_status, CASE WHEN invalid_price > 0 THEN
		'CRITICAL RULE "invalid_price" VIOLATED: Unable to load "' + @table_name + '".' ELSE NULL END AS err_detail 
		FROM #dq_metrics_crm_sales_details;

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
		MERGE silver.crm_sales_details AS tgt
		USING silver_stg.crm_sales_details AS src
			ON tgt.sls_ord_num = src.sls_ord_num
		AND tgt.sls_prd_key = src.sls_prd_key

		-- Update outdated records
		WHEN MATCHED 
			AND tgt.dwh_row_hash != src.dwh_row_hash THEN
		UPDATE SET
			tgt.sls_cust_id = src.sls_cust_id,
			tgt.sls_order_dt = src.sls_order_dt,
			tgt.sls_ship_dt = src.sls_ship_dt,
			tgt.sls_due_dt = src.sls_due_dt,
			tgt.sls_sales = src.sls_sales,
			tgt.sls_quantity = src.sls_quantity,
			tgt.sls_price = src.sls_price,
			tgt.dwh_step_run_id = src.dwh_step_run_id,
			tgt.dwh_raw_row = src.dwh_raw_row,
			tgt.dwh_row_hash = src.dwh_row_hash

		-- Insert new records
		WHEN NOT MATCHED BY TARGET THEN
			INSERT
			(
				sls_ord_num,
				sls_prd_key,
				sls_cust_id,
				sls_order_dt,
				sls_ship_dt,
				sls_due_dt,
				sls_sales,
				sls_quantity,
				sls_price,
				dwh_step_run_id,
				dwh_raw_row,
				dwh_row_hash
			)
			VALUES
			(
				src.sls_ord_num,
				src.sls_prd_key,
				src.sls_cust_id,
				src.sls_order_dt,
				src.sls_ship_dt,
				src.sls_due_dt,
				src.sls_sales,
				src.sls_quantity,
				src.sls_price,
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
