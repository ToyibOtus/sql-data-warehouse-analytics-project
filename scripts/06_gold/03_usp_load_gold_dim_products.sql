/*
===========================================================================================================
Stored Procedure: Load Gold Table (Silver -> Gold)
===========================================================================================================
Script Purpose:
	This script loads gold table [dim_products]. It combines related business domain, and also performs 
	logging operations, by inserting vital logging details into log tables [etl_step_run] & [etl_error_log].

Parameter: @job_run_id UNIQUEIDENTIFIER = NULL

Usage: EXEC gold.usp_load_gold_dim_products;

Note:
	* Running this script independently, return NULL to job_run_id (parameter) in log tables.
	* Ensure to run the master procedure as it assigns similar job_run_id across all tables & layers
	  within the same ETL run, and thus allowing for easy traceability & debugging.
===========================================================================================================
*/
CREATE OR ALTER PROCEDURE gold.usp_load_gold_dim_products @job_run_id UNIQUEIDENTIFIER = NULL AS
BEGIN
	-- Abort transaction on severe error
	SET XACT_ABORT ON;

	-- Declare and map values to variables where necessary
	DECLARE
	@step_run_id UNIQUEIDENTIFIER = NEWID(),
	@layer NVARCHAR(50) = 'gold',
	@table_name NVARCHAR(50) = 'dim_products',
	@step_name NVARCHAR(50) = 'usp_load_gold_dim_products',
	@start_time DATETIME,
	@end_time DATETIME,
	@step_duration INT,
	@step_status NVARCHAR(50) = 'RUNNING',
	@rows_source INT,
	@rows_loaded INT,
	@rows_diff INT,
	@source_path NVARCHAR(1000) = 'silver.crm_prd_info + silver.erp_px_cat_g1v2';

	-- Capture start time
	SET @start_time = GETDATE();

	-- Load log table etl_step_run
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
		-- Drop temp table #dim_products if exists
		DROP TABLE IF EXISTS #dim_products;

		-- Perform data_integration
		WITH data_integration AS
		(
		SELECT
			cpi.prd_id AS product_id,
			cpi.prd_key AS product_number,
			cpi.prd_nm AS product_name,
			cpi.prd_line AS product_line,
			cpi.cat_id AS category_id,
			pcg.cat AS category,
			pcg.subcat AS subcategory,
			pcg.maintenance,
			cpi.prd_cost AS product_cost,
			cpi.prd_start_dt AS product_start_date
		FROM silver.crm_prd_info cpi
		LEFT JOIN silver.erp_px_cat_g1v2 pcg
		ON cpi.cat_id = pcg.id
		WHERE prd_end_dt IS NULL
		)
		-- Generate meatadata columns
		, metadata_columns AS
		(
		SELECT
			product_id,
			product_number,
			product_name,
			product_line,
			category_id,
			category,
			subcategory,
			maintenance,
			product_cost,
			product_start_date,
			@step_run_id AS dwh_step_run_id,
			CONCAT_WS('|', 
			product_id,
			product_number,
			product_name,
			product_line,
			category_id,
			category,
			subcategory,
			maintenance,
			product_cost,
			product_start_date) AS dwh_raw_row,
			HASHBYTES('SHA2_256', CONCAT_WS('|',
			COALESCE(CAST(product_id AS VARBINARY(64)), '~'),
			COALESCE(CAST(product_number AS VARBINARY(64)), '~'),
			COALESCE(CAST(product_name AS VARBINARY(64)), '~'),
			COALESCE(CAST(product_line AS VARBINARY(64)), '~'),
			COALESCE(CAST(category_id AS VARBINARY(64)), '~'),
			COALESCE(CAST(category AS VARBINARY(64)), '~'),
			COALESCE(CAST(subcategory AS VARBINARY(64)), '~'),
			COALESCE(CAST(maintenance AS VARBINARY(64)), '~'),
			COALESCE(CAST(product_cost AS VARBINARY(64)), '~'),
			COALESCE(CAST(product_start_date AS VARBINARY(64)), '~'))) AS dwh_row_hash
		FROM data_integration
		)
		-- Load into temp table
		SELECT
			mc.product_id,
			mc.product_number,
			mc.product_name,
			mc.product_line,
			mc.category_id,
			mc.category,
			mc.subcategory,
			mc.maintenance,
			mc.product_cost,
			mc.product_start_date,
			mc.dwh_step_run_id,
			mc.dwh_raw_row,
			mc.dwh_row_hash
			INTO #dim_products
		FROM metadata_columns mc
		LEFT JOIN gold.dim_products dp
		ON mc.product_id = dp.product_id
		AND mc.dwh_row_hash = dp.dwh_row_hash
		WHERE dp.dwh_row_hash IS NULL;

		-- Retrieve count of rows from temp table
		SELECT @rows_source = COUNT(*) FROM #dim_products;

		-- Stop transaction if total row count equals Nulls or zero
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
					msg = 'No new records from source path "' + @source_path + '".'
				WHERE step_run_id = @step_run_id;

			RETURN;
		END;

		-- Begin transaction
		BEGIN TRAN;

		-- Merge tables
		MERGE gold.dim_products tgt
		USING #dim_products src
			ON tgt.product_id = src.product_id

		-- Update outdated records
		WHEN MATCHED 
			AND tgt.dwh_row_hash != src.dwh_row_hash THEN
		UPDATE SET
			tgt.product_number = src.product_number,
			tgt.product_name = src.product_name,
			tgt.product_line = src.product_line,
			tgt.category_id = src.category_id,
			tgt.category = src.category,
			tgt.subcategory = src.subcategory,
			tgt.maintenance = src.maintenance,
			tgt.product_cost = src.product_cost,
			tgt.product_start_date = src.product_start_date,
			tgt.dwh_step_run_id = src.dwh_step_run_id,
			tgt.dwh_raw_row = src.dwh_raw_row,
			tgt.dwh_row_hash = src.dwh_row_hash

		-- Load new records
		WHEN NOT MATCHED BY TARGET THEN
			INSERT
			(
				product_id,
				product_number,
				product_name,
				product_line,
				category_id,
				category,
				subcategory,
				maintenance,
				product_cost,
				product_start_date,
				dwh_step_run_id,
				dwh_raw_row,
				dwh_row_hash
			)
			VALUES
			(
				src.product_id,
				src.product_number,
				src.product_name,
				src.product_line,
				src.category_id,
				src.category,
				src.subcategory,
				src.maintenance,
				src.product_cost,
				src.product_start_date,
				src.dwh_step_run_id,
				src.dwh_raw_row,
				src.dwh_row_hash	
			);
		-- Retrive rows loaded
		SET @rows_loaded = @@ROWCOUNT;

		-- Finalize transaction on success
		COMMIT TRAN;

		-- Map values to variables
		SET @end_time = GETDATE();
		SET @step_duration = DATEDIFF(second, @start_time, @end_time);
		SET @step_status = 'SUCCESS';
		SET @rows_diff = @rows_source - @rows_loaded;

		-- Update log table on success
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

		-- Map zero to rows source if NULL or zero
		IF @rows_source IS NULL SET @rows_source = 0;

		-- Rollback transaction on error
		IF @@TRANCOUNT > 0 ROLLBACK TRAN;

		-- Map zero to rows loaded if NULL or zero
		IF @rows_loaded IS NULL SET @rows_loaded = 0;

		-- Calculate rows diff
		SET @rows_diff = @rows_source - @rows_loaded;

		-- Update log table on failure
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

		-- Load log table with error details
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
