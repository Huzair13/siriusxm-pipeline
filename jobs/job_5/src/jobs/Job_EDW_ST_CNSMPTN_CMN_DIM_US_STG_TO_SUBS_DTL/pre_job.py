"""
Pre-job tasks for the Job_EDW_ST_CNSMPTN_CMN_DIM_US_STG_TO_SUBS_DTL job.

This module contains functions for the pre-job phase of the Job_EDW_ST_CNSMPTN_CMN_DIM_US_STG_TO_SUBS_DTL job.
"""
import logging
from typing import Dict, Any, Optional, Tuple

from src.context import context
from src.utils import redshift_utils, ssm_utils, logging_utils
from src.joblets.jl_Frmwrk_EDW_LOAD_CONTEXT import main_joblet as load_context_joblet
from src.jobs.Job_Frmwrk_EDW_BATCH_DETAIL_START import main_job as batch_detail_start

logger = logging.getLogger(__name__)

def establish_database_connections(config: Dict[str, Any]) -> Tuple[Any, Any]:
    """
    Establish connections to Redshift databases.
    
    Args:
        config: Configuration dictionary
        
    Returns:
        Tuple of (datamart_conn, ods_conn)
        
    Raises:
        Exception: If connections cannot be established
    """
    try:
        # Get database configuration
        datamart_config = config.get('database', {}).get('redshift_datamart', {})
        ods_config = config.get('database', {}).get('redshift_ods', {})
        
        # Get database connection parameters from SSM Parameter Store
        host = ssm_utils.get_parameter("/edo/" + context.batch_env + "/redshift/host")
        port = int(ssm_utils.get_parameter("/edo/" + context.batch_env + "/redshift/port"))
        database = ssm_utils.get_parameter("/edo/" + context.batch_env + "/redshift/database")
        
        # Connect to Redshift datamart
        datamart_user = datamart_config.get('user', 'edw_datamart_etl')
        datamart_password = ssm_utils.get_parameter(datamart_config.get('parameter_store_path', '/edo/dev/redshift/datamart/edw_datamart_stg'), True)
        datamart_conn = redshift_utils.get_redshift_connection(host, port, database, datamart_user, datamart_password)
        
        # Connect to Redshift ODS
        ods_user = ods_config.get('user', 'edw_datamart_etl')
        ods_password = ssm_utils.get_parameter(ods_config.get('parameter_store_path', '/edo/dev/redshift/ods/edw_ods'), True)
        ods_conn = redshift_utils.get_redshift_connection(host, port, database, ods_user, ods_password)
        
        logger.info("Successfully established database connections")
        
        return datamart_conn, ods_conn
    except Exception as e:
        logger.error(f"Failed to establish database connections: {str(e)}")
        raise

def run_pre_job(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Run pre-job tasks for the Job_EDW_ST_CNSMPTN_CMN_DIM_US_STG_TO_SUBS_DTL job.
    
    Args:
        config: Configuration dictionary
        
    Returns:
        Dictionary with database connections
        
    Raises:
        Exception: If pre-job tasks fail
    """
    try:
        # Load context variables
        load_context_joblet.run_joblet()
        
        # Set source and target table names
        context.batch_src_tbl_name = context.stg_src_table
        context.batch_tgt_tbl_name = context.stg_table_dt_subs_dtl
        
        # Establish database connections
        datamart_conn, ods_conn = establish_database_connections(config)
        
        # Run batch detail start job
        batch_detail_start.run_main_job(config)
        
        logger.info("Pre-job tasks completed successfully")
        
        return {
            "datamart_conn": datamart_conn,
            "ods_conn": ods_conn
        }
    except Exception as e:
        logger.error(f"Pre-job tasks failed: {str(e)}")
        raise