/*
===========================================================================================================
Stored Procedure: Load Gold Table (Silver -> Gold)
===========================================================================================================
Script Purpose:
	This script loads gold table [dim_customers]. It combines related business domain, and also performs 
	logging operations, by inserting vital logging details into log tables [etl_step_run] & [etl_error_log].

Parameter: @job_run_id UNIQUEIDENTIFIER = NULL

Usage: EXEC gold.usp_load_gold_dim_customers;

Note:
	* Running this script independently, return NULL to job_run_id (parameter) in log tables.
	* Ensure to run the master procedure as it assigns similar job_run_id across all tables & layers
	  within the same ETL run, and thus allowing for easy traceability & debugging.
===========================================================================================================
*/
CREATE OR ALTER PROCEDURE gold.usp_load_gold_dim_customers @job_run_id UNIQUEIDENTIFIER = NULL AS
BEGIN
	-- Abort transaction on severe error
	SET XACT_ABORT ON;

	-- Declare and map values to variables where necessary
	DECLARE
	@step_run_id UNIQUEIDENTIFIER = NEWID(),
	@layer NVARCHAR(50) = 'gold',
	@table_name NVARCHAR(50) = 'dim_customers',
	@step_name NVARCHAR(50) = 'usp_load_gold_dim_customers',
	@start_time DATETIME,
	@end_time DATETIME,
	@step_duration INT,
	@step_status NVARCHAR(50) = 'RUNNING',
	@rows_source INT,
	@rows_loaded INT,
	@rows_diff INT,
	@source_path NVARCHAR(1000) = 'silver.crm_cust_info + silver.erp_cust_az12 + silver.erp_loc_a101';

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
		-- Drop temp table #dim_customers if exists
		DROP TABLE IF EXISTS #dim_customers;

		-- Perform data_integration
		WITH data_integration AS
		(
		SELECT 
			ci.cst_id AS customer_id,
			ci.cst_key AS customer_number,
			ci.cst_first_name AS first_name,
			ci.cst_last_name AS last_name,
			la.cntry AS country,
			CASE 
				WHEN ci.cst_gndr = 'Unknown' AND ca.gen IS NOT NULL THEN ca.gen
				ELSE ci.cst_gndr
			END AS gender,
			ci.cst_marital_status AS marital_status,
			ca.bdate AS birth_date,
			ci.cst_create_date AS create_date
		FROM silver.crm_cust_info ci
		LEFT JOIN silver.erp_cust_az12 ca
		ON ci.cst_key = ca.cid
		LEFT JOIN silver.erp_loc_a101 la
		ON ci.cst_key = la.cid
		)
		-- Generate meatadata columns
		, metadata_columns AS
		(
		SELECT
			customer_id,
			customer_number,
			first_name,
			last_name,
			country,
			gender,
			marital_status,
			birth_date,
			create_date,
			@step_run_id AS dwh_step_run_id,
			CONCAT_WS('|', 
			customer_id,
			customer_number,
			first_name,
			last_name,
			country,
			gender,
			marital_status,
			birth_date,
			create_date) AS dwh_raw_row,
			HASHBYTES('SHA2_256', CONCAT_WS('|', 
			COALESCE(CAST(customer_id AS VARBINARY(64)), '~'),
			COALESCE(CAST(customer_number AS VARBINARY(64)), '~'),
			COALESCE(CAST(first_name AS VARBINARY(64)), '~'),
			COALESCE(CAST(last_name AS VARBINARY(64)), '~'),
			COALESCE(CAST(country AS VARBINARY(64)), '~'),
			COALESCE(CAST(gender AS VARBINARY(64)), '~'),
			COALESCE(CAST(marital_status AS VARBINARY(64)), '~'),
			COALESCE(CAST(birth_date AS VARBINARY(64)), '~'),
			COALESCE(CAST(create_date AS VARBINARY(64)), '~'))) AS dwh_row_hash
		FROM data_integration
		)
		-- Load into temp table
		SELECT
			mc.customer_id,
			mc.customer_number,
			mc.first_name,
			mc.last_name,
			mc.country,
			mc.gender,
			mc.marital_status,
			mc.birth_date,
			mc.create_date,
			mc.dwh_step_run_id,
			mc.dwh_raw_row,
			mc.dwh_row_hash
			INTO #dim_customers
		FROM metadata_columns mc
		LEFT JOIN gold.dim_customers dc
		ON mc.customer_id = dc.customer_id 
		AND mc.dwh_row_hash = dc.dwh_row_hash
		WHERE dc.dwh_row_hash IS NULL;

		-- Retrieve count of rows from temp table
		SELECT @rows_source = COUNT(*) FROM #dim_customers;

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
		MERGE gold.dim_customers tgt
		USING #dim_customers src
			ON tgt.customer_id = src.customer_id

		-- Update outdated records
		WHEN MATCHED 
			AND tgt.dwh_row_hash != src.dwh_row_hash THEN
		UPDATE SET
				tgt.customer_number = src.customer_number,
				tgt.first_name = src.first_name,
				tgt.last_name = src.last_name,
				tgt.country = src.country,
				tgt.gender = src.gender,
				tgt.marital_status = src.marital_status,
				tgt.birth_date = src.birth_date,
				tgt.create_date = src.create_date,
				tgt.dwh_step_run_id = src.dwh_step_run_id,
				tgt.dwh_raw_row = src.dwh_raw_row,
				tgt.dwh_row_hash = src.dwh_row_hash

		-- Load new records
		WHEN NOT MATCHED BY TARGET THEN
			INSERT
			(
				customer_id,
				customer_number,
				first_name,
				last_name,
				country,
				gender,
				marital_status,
				birth_date,
				create_date,
				dwh_step_run_id,
				dwh_raw_row,
				dwh_row_hash
			)
			VALUES
			(
				src.customer_id,
				src.customer_number,
				src.first_name,
				src.last_name,
				src.country,
				src.gender,
				src.marital_status,
				src.birth_date,
				src.create_date,
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
