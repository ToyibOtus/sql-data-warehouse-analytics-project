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
-- Create log table [audit].etl_job_run
CREATE TABLE [audit].etl_job_run
(
	job_run_id UNIQUEIDENTIFIER NOT NULL,
	pipeline_name NVARCHAR(50) NOT NULL,
	start_time DATETIME NOT NULL,
	end_time DATETIME,
	job_run_duration_seconds INT,
	job_run_status NVARCHAR(50) NOT NULL,
	err_message NVARCHAR(MAX),
	trigger_type NVARCHAR(50),
	CONSTRAINT pk_etl_job_run PRIMARY KEY(job_run_id)
);

-- Create log table [audit].etl_step_run
CREATE TABLE [audit].etl_step_run
(
	step_run_id UNIQUEIDENTIFIER NOT NULL,
	job_run_id UNIQUEIDENTIFIER,
	layer NVARCHAR(50) NOT NULL,
	table_name NVARCHAR(50) NOT NULL,
	step_name NVARCHAR(50) NOT NULL,
	start_time DATETIME NOT NULL,
	end_time DATETIME,
	step_run_duration_seconds INT,
	step_run_status NVARCHAR(50),
	rows_source INT,
	rows_loaded INT,
	rows_diff INT,
	source_path NVARCHAR(MAX),
	err_message NVARCHAR(MAX),
	CONSTRAINT pk_etl_step_run PRIMARY KEY(step_run_id),
	CONSTRAINT fk_etl_step_run_etl_job_run FOREIGN KEY(job_run_id) REFERENCES [audit].etl_job_run(job_run_id)
);

-- Create log table [audit].etl_error_log
CREATE TABLE [audit].etl_error_log
(
	error_run_id UNIQUEIDENTIFIER DEFAULT NEWID() NOT NULL,
	job_run_id UNIQUEIDENTIFIER,
	step_run_id UNIQUEIDENTIFIER NOT NULL,
	err_procedure NVARCHAR(50) NOT NULL,
	err_timestamp DATETIME DEFAULT GETDATE() NOT NULL,
	err_number INT NOT NULL,
	err_message NVARCHAR(MAX) NOT NULL,
	err_state INT NOT NULL,
	err_line INT NOT NULL,
	err_severity INT NOT NULL,
	CONSTRAINT pk_etl_error_log PRIMARY KEY(error_run_id),
	CONSTRAINT fk_etl_error_log_etl_job_run FOREIGN KEY(job_run_id) REFERENCES [audit].etl_job_run(job_run_id),
	CONSTRAINT fk_etl_error_log_etl_step_run FOREIGN KEY(step_run_id) REFERENCES [audit].etl_step_run(step_run_id)
);

-- Create log table [audit].etl_data_quality
CREATE TABLE [audit].etl_data_quality
(
	dq_run_id UNIQUEIDENTIFIER DEFAULT NEWID() NOT NULL,
	job_run_id UNIQUEIDENTIFIER,
	step_run_id UNIQUEIDENTIFIER NOT NULL,
	dq_timestamp DATETIME DEFAULT GETDATE() NOT NULL,
	dq_layer NVARCHAR(50) NOT NULL,
	dq_table_name NVARCHAR(50) NOT NULL,
	dq_check_name NVARCHAR(50) NOT NULL,
	rows_checked INT,
	rows_failed INT,
	dq_status NVARCHAR(50) NOT NULL,
	err_detail NVARCHAR(1000),
	CONSTRAINT pk_etl_data_quality PRIMARY KEY(dq_run_id),
	CONSTRAINT fk_etl_data_quality_etl_job_run FOREIGN KEY(job_run_id) REFERENCES [audit].etl_job_run(job_run_id),
	CONSTRAINT fk_etl_data_quality_etl_step_run FOREIGN KEY(step_run_id) REFERENCES [audit].etl_step_run(step_run_id)
);
