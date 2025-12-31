/*
===========================================================================================================
Stored Procedure: Load Gold Table (Silver -> Gold)
===========================================================================================================
Script Purpose:
	This script loads gold table [fact_sales]. It combines related business domain, and also performs 
	logging operations, by inserting vital logging details into log tables [etl_step_run] & [etl_error_log].

Parameter: @job_run_id UNIQUEIDENTIFIER = NULL

Usage: EXEC gold.usp_load_gold_fact_sales;

Note:
	* Running this script independently assigns a job_run_id local to this procedure.
	* Ensure to run the master procedure as it assigns similar job_run_id across all tables & layers
	  within the same ETL run, and thus allowing for easy traceability & debugging.
===========================================================================================================
*/
CREATE OR ALTER PROCEDURE gold.usp_load_gold_fact_sales @job_run_id UNIQUEIDENTIFIER = NULL AS
BEGIN
	-- Abort transaction on severe error
	SET XACT_ABORT ON;

	-- Declare and map values to variables where necessary
	DECLARE
	@step_run_id UNIQUEIDENTIFIER = NEWID(),
	@layer NVARCHAR(50) = 'gold',
	@table_name NVARCHAR(50) = 'fact_sales',
	@step_name NVARCHAR(50) = 'usp_load_gold_fact_sales',
	@start_time DATETIME,
	@end_time DATETIME,
	@step_duration INT,
	@step_status NVARCHAR(50) = 'RUNNING',
	@rows_source INT,
	@rows_loaded INT,
	@rows_diff INT,
	@source_path NVARCHAR(1000) = 'silver.crm_sales_details + gold.dim_products + gold.dim_customers';

	-- Map value to job_run_id if NULL
	IF @job_run_id IS NULL SET @job_run_id = NEWID();

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
		-- Drop temp table #fact_sales if exists
		DROP TABLE IF EXISTS #fact_sales;

		-- Perform data_integration
		WITH data_integration AS
		(
		SELECT
			sd.sls_ord_num AS order_number,
			dp.product_key,
			dc.customer_key,
			sd.sls_order_dt AS order_date,
			sd.sls_ship_dt AS shipping_date,
			sd.sls_due_dt AS due_date,
			sd.sls_sales AS sales,
			sd.sls_quantity AS quantity,
			sd.sls_price AS price
		FROM silver.crm_sales_details sd
		LEFT JOIN gold.dim_products dp
		ON sd.sls_prd_key = dp.product_number
		LEFT JOIN gold.dim_customers dc
		ON sd.sls_cust_id = dc.customer_id
		)
		-- Generate meatadata columns
		, metadata_columns AS
		(
		SELECT
			order_number,
			product_key,
			customer_key,
			order_date,
			shipping_date,
			due_date,
			sales,
			quantity,
			price,
			@job_run_id AS dwh_job_run_id,
			CONCAT_WS('|',
			order_number,
			product_key,
			customer_key,
			order_date,
			shipping_date,
			due_date,
			sales,
			quantity,
			price) AS dwh_raw_row,
			HASHBYTES('SHA2_256', CONCAT_WS('|',
			COALESCE(CAST(order_number AS VARBINARY(64)), '~'),
			COALESCE(CAST(product_key AS VARBINARY(64)), '~'),
			COALESCE(CAST(customer_key AS VARBINARY(64)), '~'),
			COALESCE(CAST(order_date AS VARBINARY(64)), '~'),
			COALESCE(CAST(shipping_date AS VARBINARY(64)), '~'),
			COALESCE(CAST(due_date AS VARBINARY(64)), '~'),
			COALESCE(CAST(sales AS VARBINARY(64)), '~'),
			COALESCE(CAST(quantity AS VARBINARY(64)), '~'),
			COALESCE(CAST(price AS VARBINARY(64)), '~'))) AS dwh_row_hash
		FROM data_integration
		)
		-- Load into temp table
		SELECT
			mc.order_number,
			mc.product_key,
			mc.customer_key,
			mc.order_date,
			mc.shipping_date,
			mc.due_date,
			mc.sales,
			mc.quantity,
			mc.price,
			mc.dwh_job_run_id,
			mc.dwh_raw_row,
			mc.dwh_row_hash
			INTO #fact_sales
		FROM metadata_columns mc
		LEFT JOIN gold.fact_sales fs
		ON mc.order_number = fs.order_number
		AND mc.product_key = fs.product_key
		AND mc.dwh_row_hash = fs.dwh_row_hash
		WHERE fs.dwh_row_hash IS NULL;

		-- Retrieve count of rows from temp table
		SELECT @rows_source = COUNT(*) FROM #fact_sales;

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
		MERGE gold.fact_sales tgt
		USING #fact_sales src
			ON tgt.order_number = src.order_number
			AND tgt.product_key = src.product_key

		-- Load new records
		WHEN NOT MATCHED BY TARGET THEN
			INSERT
			(
				order_number,
				product_key,
				customer_key,
				order_date,
				shipping_date,
				due_date,
				sales,
				quantity,
				price,
				dwh_job_run_id,
				dwh_raw_row,
				dwh_row_hash
			)
			VALUES
			(
				src.order_number,
				src.product_key,
				src.customer_key,
				src.order_date,
				src.shipping_date,
				src.due_date,
				src.sales,
				src.quantity,
				src.price,
				src.dwh_job_run_id,
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
