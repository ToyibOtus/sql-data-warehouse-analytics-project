/*
=====================================================================================================
User Stored Procedure: Load Landing Table (CSV file -> Landing)
=====================================================================================================
Script Purpose:
	This script loads [landing.erp_loc_a101]. It also performs logging operations by loading vital
	log details into log tables [etl_step_run] & [etl_error_log].

Parameter: @job_run_id UNIQUEIDENTIFIER = NULL

Usage: EXEC landing.usp_load_landing_erp_loc_a101;

Note:
	The default value of job_run_id is NULL if it is run independently.
	Ensure to run the master stored procedure as it performs a full ETL run and assigns same 
	job_run_id across all layers and tables within the same run.
=====================================================================================================
*/
CREATE OR ALTER PROCEDURE landing.usp_load_landing_erp_loc_a101 @job_run_id UNIQUEIDENTIFIER = NULL AS
BEGIN
	-- Abort transaction on severe error
	SET XACT_ABORT ON;

	-- Declare and assign values to variables
	DECLARE
	@step_run_id UNIQUEIDENTIFIER = NEWID(),
	@layer NVARCHAR(50) = 'landing',
	@table_name NVARCHAR(50) = 'erp_loc_a101',
	@step_name NVARCHAR(50) = 'usp_load_landing_erp_loc_a101',
	@start_time DATETIME,
	@end_time DATETIME,
	@step_duration INT,
	@step_status NVARCHAR(50) = 'RUNNING',
	@rows_loaded INT,
	@file_path NVARCHAR(MAX) = 'C:\Users\PC\Documents\SQL_DataWareHouseProject\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv',
	@sql NVARCHAR(MAX);

	-- Capture start time
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
		file_path
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
		@file_path
	);

	BEGIN TRY
		-- Begin Transaction
		BEGIN TRAN;
		-- Truncate table before loading
		TRUNCATE TABLE landing.erp_loc_a101;

		-- Map string value to variable
		SET @sql = 'BULK INSERT landing.erp_loc_a101 FROM ''' + @file_path + ''' WITH (FIRST_ROW = 2, FIELDTERMINATOR = '','', TABLOCK);';
		-- Load data to table
		EXEC (@sql);

		-- Finalize transaction on success
		COMMIT TRAN;

		-- Map values to variables
		SET @end_time = GETDATE();
		SET @step_duration = DATEDIFF(second, @start_time, @end_time);
		SET @step_status = 'SUCCESS';
		SELECT @rows_loaded = COUNT(*) FROM landing.erp_loc_a101;

		-- Update log table on success
		UPDATE [audit].etl_step_run
		SET
			end_time = @end_time,
			step_run_duration_seconds = @step_duration,
			step_run_status = @step_status,
			rows_loaded = @rows_loaded
		WHERE step_run_id = @step_run_id;
	END TRY

	BEGIN CATCH
		-- Map values to variables in catch block
		SET @end_time = GETDATE();
		SET @step_duration = DATEDIFF(second, @start_time, @end_time);
		SET @step_status = 'FAILED';

		-- Rollback transaction on error
		IF @@TRANCOUNT > 0 ROLLBACK TRAN;

		-- Set rows loaded to 0 when NULL
		IF @rows_loaded IS NULL SET @rows_loaded = 0;

		-- Update log table on failure
		UPDATE [audit].etl_step_run
		SET
			end_time = @end_time,
			step_run_duration_seconds = @step_duration,
			step_run_status = @step_status,
			rows_loaded = @rows_loaded,
			err_message = ERROR_MESSAGE()
		WHERE step_run_id = @step_run_id;
	
		-- Insert error_details into log table
		INSERT INTO [audit].etl_error_log
		(
			job_run_id,
			step_run_id,
			err_procedure,
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
			ERROR_MESSAGE(),
			ERROR_STATE(),
			ERROR_LINE(),
			ERROR_SEVERITY()
		);
	END CATCH;
END;
