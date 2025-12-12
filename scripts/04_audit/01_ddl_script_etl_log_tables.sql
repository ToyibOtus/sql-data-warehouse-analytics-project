/*
======================================================================================
DDL Script: Build ETL Log Tables
======================================================================================
Script Purpose:
	This script builds and designs the structure of 4 ETL log tables:
	[etl_job_run], [etl_step_run], [etl_error_log], and [etl_data_quality].

	Run this script to change the structure of your ETL log tables.
======================================================================================
*/
-- Drop Foreign key [fk_etl_step_run_etl_job_run]
IF EXISTS(SELECT 1 FROM sys.foreign_keys WHERE name = 'fk_etl_step_run_etl_job_run')
ALTER TABLE [audit].etl_step_run
DROP CONSTRAINT fk_etl_step_run_etl_job_run;

-- Drop Foreign key [fk_etl_error_log_etl_job_run]
IF EXISTS(SELECT 1 FROM sys.foreign_keys WHERE name = 'fk_etl_error_log_etl_job_run')
ALTER TABLE [audit].etl_error_log
DROP CONSTRAINT fk_etl_error_log_etl_job_run;

-- Drop Foreign key [fk_etl_data_quality_etl_job_run]
IF EXISTS(SELECT 1 FROM sys.foreign_keys WHERE name = 'fk_etl_data_quality_etl_job_run')
ALTER TABLE [audit].etl_data_quality
DROP CONSTRAINT fk_etl_data_quality_etl_job_run;

-- Drop Foreign key [fk_etl_error_log_etl_step_run]
IF EXISTS(SELECT 1 FROM sys.foreign_keys WHERE name = 'fk_etl_error_log_etl_step_run')
ALTER TABLE [audit].etl_error_log
DROP CONSTRAINT fk_etl_error_log_etl_step_run;

-- Drop Foreign key [fk_etl_data_quality_etl_step_run]
IF EXISTS(SELECT 1 FROM sys.foreign_keys WHERE name = 'fk_etl_data_quality_etl_step_run')
ALTER TABLE [audit].etl_data_quality
DROP CONSTRAINT fk_etl_data_quality_etl_step_run;

-- Create log table [audit].etl_job_run
IF OBJECT_ID('[audit].etl_job_run', 'U') IS NOT NULL
DROP TABLE [audit].etl_job_run;
GO

CREATE TABLE [audit].etl_job_run
(
	job_run_id UNIQUEIDENTIFIER NOT NULL,
	pipeline_name NVARCHAR(50) NOT NULL,
	start_time DATETIME NOT NULL,
	end_time DATETIME NOT NULL,
	job_run_duration_seconds INT NOT NULL,
	job_run_status NVARCHAR(50) NOT NULL,
	err_message NVARCHAR(MAX),
	trigger_type NVARCHAR(50),
	CONSTRAINT pk_etl_job_run PRIMARY KEY(job_run_id)
);

-- Create log table [audit].etl_step_run
IF OBJECT_ID('[audit].etl_step_run', 'U') IS NOT NULL
DROP TABLE [audit].etl_step_run;
GO

CREATE TABLE [audit].etl_step_run
(
	step_run_id UNIQUEIDENTIFIER NOT NULL,
	job_run_id UNIQUEIDENTIFIER NOT NULL,
	layer NVARCHAR(50) NOT NULL,
	table_name NVARCHAR(50) NOT NULL,
	step_name NVARCHAR(50) NOT NULL,
	start_time DATETIME NOT NULL,
	end_time DATETIME NOT NULL,
	step_run_duration_seconds INT NOT NULL,
	step_run_status NVARCHAR(50) NOT NULL,
	rows_source INT,
	rows_loaded INT,
	rows_diff INT,
	err_message NVARCHAR(MAX),
	CONSTRAINT pk_etl_step_run PRIMARY KEY(step_run_id),
	CONSTRAINT fk_etl_step_run_etl_job_run FOREIGN KEY(job_run_id) REFERENCES [audit].etl_job_run(job_run_id)
);

-- Create log table [audit].etl_error_log
IF OBJECT_ID('[audit].etl_error_log', 'U') IS NOT NULL
DROP TABLE [audit].etl_error_log;
GO

CREATE TABLE [audit].etl_error_log
(
	error_run_id UNIQUEIDENTIFIER NOT NULL,
	job_run_id UNIQUEIDENTIFIER NOT NULL,
	step_run_id UNIQUEIDENTIFIER NOT NULL,
	err_procedure NVARCHAR(50) NOT NULL,
	err_timestamp DATETIME NOT NULL,
	err_message NVARCHAR(MAX) NOT NULL,
	err_state INT NOT NULL,
	err_line INT NOT NULL,
	err_severity INT NOT NULL,
	CONSTRAINT pk_etl_error_log PRIMARY KEY(error_run_id),
	CONSTRAINT fk_etl_error_log_etl_job_run FOREIGN KEY(job_run_id) REFERENCES [audit].etl_job_run(job_run_id),
	CONSTRAINT fk_etl_error_log_etl_step_run FOREIGN KEY(step_run_id) REFERENCES [audit].etl_step_run(step_run_id)
);

-- Create log table [audit].etl_data_quality
IF OBJECT_ID('[audit].etl_data_quality', 'U') IS NOT NULL
DROP TABLE [audit].etl_data_quality;
GO

CREATE TABLE [audit].etl_data_quality
(
	dq_run_id UNIQUEIDENTIFIER NOT NULL,
	job_run_id UNIQUEIDENTIFIER NOT NULL,
	step_run_id UNIQUEIDENTIFIER NOT NULL,
	dq_timestamp DATETIME NOT NULL,
	dq_check_name NVARCHAR(50) NOT NULL,
	dq_status NVARCHAR(50) NOT NULL,
	expected_rows INT NOT NULL,
	actual_rows INT NOT NULL,
	err_detail NVARCHAR(1000) NULL,
	CONSTRAINT pk_etl_data_quality PRIMARY KEY(dq_run_id),
	CONSTRAINT fk_etl_data_quality_etl_job_run FOREIGN KEY(job_run_id) REFERENCES [audit].etl_job_run(job_run_id),
	CONSTRAINT fk_etl_data_quality_etl_step_run FOREIGN KEY(step_run_id) REFERENCES [audit].etl_step_run(step_run_id)
);
