"""
Post-job tasks for the Job_EDW_ST_CNSMPTN_CMN_DIM_US_STG_TO_SUBS_DTL job.

This module contains functions for the post-job phase of the Job_EDW_ST_CNSMPTN_CMN_DIM_US_STG_TO_SUBS_DTL job.
"""
import logging
from typing import Dict, Any, Optional

from src.context import context
from src.utils import redshift_utils, logging_utils
from src.jobs.Job_Frmwrk_EDW_BATCH_DETAIL_CLOSE import main_job as batch_detail_close

logger = logging.getLogger(__name__)

def close_database_connections(pre_job_results: Dict[str, Any]) -> None:
    """
    Close database connections.
    
    Args:
        pre_job_results: Results from pre-job tasks
        
    Raises:
        Exception: If connections cannot be closed
    """
    try:
        # Get database connections from pre-job results
        datamart_conn = pre_job_results.get("datamart_conn")
        ods_conn = pre_job_results.get("ods_conn")
        
        # Close connections if they exist
        if datamart_conn:
            datamart_conn.close()
            logger.info("Closed Redshift datamart connection")
        
        if ods_conn:
            ods_conn.close()
            logger.info("Closed Redshift ODS connection")
    except Exception as e:
        logger.error(f"Failed to close database connections: {str(e)}")
        raise

def run_post_job(pre_job_results: Dict[str, Any], main_job_results: Dict[str, Any], config: Dict[str, Any]) -> None:
    """
    Run post-job tasks for the Job_EDW_ST_CNSMPTN_CMN_DIM_US_STG_TO_SUBS_DTL job.
    
    Args:
        pre_job_results: Results from pre-job tasks
        main_job_results: Results from main job tasks
        config: Configuration dictionary
        
    Raises:
        Exception: If post-job tasks fail
    """
    try:
        # Run batch detail close job
        batch_detail_close.run_main_job(config)
        
        # Close database connections
        close_database_connections(pre_job_results)
        
        logger.info("Post-job tasks completed successfully")
    except Exception as e:
        logger.error(f"Post-job tasks failed: {str(e)}")
        raise