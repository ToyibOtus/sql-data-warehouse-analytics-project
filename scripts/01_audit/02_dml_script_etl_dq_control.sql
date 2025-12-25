/*
===============================================================================================
DML Script: Load etl_data_quality_control
===============================================================================================
Script Purpose:
  This script loads etl_quality_control with vital dq metrics, along with their degree of
  severity.
===============================================================================================
*/
INSERT INTO [audit].etl_data_quality_control
(
	dq_layer,
	dq_table_name,
	dq_check_name,
	dq_severity,
	stop_on_failure,
	is_active
)
VALUES
	('silver', 'crm_cust_info', 'pk_nulls', 'CRITICAL', 1, 1),
	('silver', 'crm_cust_info', 'pk_duplicates', 'CRITICAL', 1, 1),
	('silver', 'crm_cust_info', 'cst_key_untrimmed', 'WARNING', 0, 1),
	('silver', 'crm_cust_info', 'cst_first_name_untrimmed', 'WARNING', 0, 1),
	('silver', 'crm_cust_info', 'cst_last_name_untrimmed', 'WARNING', 0, 1),
	('silver', 'crm_cust_info', 'invalid_marital_status', 'WARNING', 0, 1),
	('silver', 'crm_cust_info', 'invalid_gndr', 'WARNING', 0, 1),
	('silver', 'crm_prd_info', 'pk_nulls', 'CRITICAL', 1, 1),
	('silver', 'crm_prd_info', 'pk_duplicates', 'CRITICAL', 1, 1),
	('silver', 'crm_prd_info', 'prd_nm_untrimmed', 'WARNING', 0, 1),
	('silver', 'crm_prd_info', 'invalid_cost', 'WARNING', 0, 1),
	('silver', 'crm_prd_info', 'invalid_prd_line', 'INFO', 0, 1),
	('silver', 'crm_prd_info', 'invalid_prd_start_dt', 'WARNING', 0, 1),
	('silver', 'crm_prd_info', 'invalid_prd_end_dt', 'WARNING', 0, 1),
	('silver', 'crm_sales_details', 'sls_ord_untrimmed', 'WARNING', 0, 1),
	('silver', 'crm_sales_details', 'sls_prd_key_untrimmed', 'WARNING', 0, 1),
	('silver', 'crm_sales_details', 'sls_prd_key_nulls', 'CRITICAL', 1, 1),
	('silver', 'crm_sales_details', 'sls_cust_id_nulls', 'CRITICAL', 1, 1),
	('silver', 'crm_sales_details', 'invalid_order_dt', 'WARNING', 0, 1),
	('silver', 'crm_sales_details', 'invalid_ship_dt', 'WARNING', 0, 1),
	('silver', 'crm_sales_details', 'invalid_due_dt', 'WARNING', 0, 1),
	('silver', 'crm_sales_details', 'invalid_sales', 'CRITICAL', 1, 1),
	('silver', 'crm_sales_details', 'invalid_quantity', 'CRITICAL', 1, 1),
	('silver', 'crm_sales_details', 'invalid_price', 'CRITICAL', 1, 1),
	('silver', 'erp_cust_az12', 'pk_nulls', 'CRITICAL', 1, 1),
	('silver', 'erp_cust_az12', 'pk_duplicates', 'CRITICAL', 1, 1),
	('silver', 'erp_cust_az12', 'invalid_bdate', 'WARNING', 0, 1),
	('silver', 'erp_cust_az12', 'invalid_gen', 'WARNING', 0, 1),
	('silver', 'erp_loc_a101', 'pk_nulls', 'WARNING', 1, 1),
	('silver', 'erp_loc_a101', 'pk_duplicates', 'WARNING', 1, 1),
	('silver', 'erp_px_cat_g1v2', 'pk_nulls', 'CRITICAL', 1, 1),
	('silver', 'erp_px_cat_g1v2', 'pk_duplicates', 'CRITICAL', 1, 1),
	('silver', 'erp_px_cat_g1v2', 'cat_untrimmed', 'WARNING', 0, 1),
	('silver', 'erp_px_cat_g1v2', 'invalid_cat', 'INFO', 0, 1),
	('silver', 'erp_px_cat_g1v2', 'subcat_untrimmed', 'WARNING', 0, 1),
	('silver', 'erp_px_cat_g1v2', 'maintenance_untrimmed', 'WARNING', 0, 1),
	('silver', 'erp_px_cat_g1v2', 'invalid_maintenance', 'WARNING', 0, 1)
