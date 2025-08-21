"""
Main job tasks for the Job_Frmwrk_EDW_BATCH_DETAIL_START job.

This module contains functions for the main processing phase of the Job_Frmwrk_EDW_BATCH_DETAIL_START job.
"""
import logging
from typing import Dict, Any, Optional

from src.context import context
from src.utils import redshift_utils, ssm_utils

logger = logging.getLogger(__name__)

def get_batch_id(conn, subject_area_id: str) -> int:
    """
    Get the batch ID for the job.
    
    Args:
        conn: Redshift connection
        subject_area_id: Subject area ID
        
    Returns:
        Batch ID
        
    Raises:
        Exception: If the batch ID cannot be retrieved
    """
    try:
        query = f"""
        select max(batch_id) as batch_id from edw_ods.batch_audit
        where process_status_cd='In Progress' and subject_area_id = {subject_area_id}
        """
        
        result = redshift_utils.fetch_one(conn, query)
        
        if result and result[0]:
            batch_id = result[0]
            logger.info(f"Retrieved batch ID: {batch_id}")
            return batch_id
        else:
            logger.error("No batch ID found")
            raise ValueError("No batch ID found")
    except Exception as e:
        logger.error(f"Failed to get batch ID: {str(e)}")
        raise

def insert_batch_audit_detail(conn, batch_id: int, job_name: str, src_table_nm: str, tgt_table_nm: str) -> None:
    """
    Insert a record into the batch audit detail table.
    
    Args:
        conn: Redshift connection
        batch_id: Batch ID
        job_name: Job name
        src_table_nm: Source table name
        tgt_table_nm: Target table name
        
    Raises:
        Exception: If the record cannot be inserted
    """
    try:
        query = f"""
        begin;
        insert into edw_ods.batch_audit_detail (batch_id, job_nm, process_status_cd, batch_detail_start_ts, file_nm, src_table_nm, tgt_table_nm) 
        values ({batch_id}, '{job_name}', 'In Progress', current_timestamp, 'N/A', '{src_table_nm}', '{tgt_table_nm}');
        end;
        """
        
        redshift_utils.execute_query(conn, query)
        logger.info(f"Inserted batch audit detail record for job {job_name}")
    except Exception as e:
        logger.error(f"Failed to insert batch audit detail record: {str(e)}")
        raise

def run_main_job(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Run main job tasks for the Job_Frmwrk_EDW_BATCH_DETAIL_START job.
    
    Args:
        config: Configuration dictionary
        
    Returns:
        Dictionary of job results
        
    Raises:
        Exception: If main job tasks fail
    """
    try:
        # Get database configuration
        db_config = config.get('database', {}).get('redshift_datamart', {})
        
        # Get database connection parameters from SSM Parameter Store
        host = ssm_utils.get_parameter("/edo/" + context.batch_env + "/redshift/host")
        port = int(ssm_utils.get_parameter("/edo/" + context.batch_env + "/redshift/port"))
        database = ssm_utils.get_parameter("/edo/" + context.batch_env + "/redshift/database")
        user = db_config.get('user', 'edw_datamart_etl')
        password = ssm_utils.get_parameter(db_config.get('parameter_store_path', '/edo/dev/redshift/datamart/edw_datamart_stg'), True)
        
        # Connect to Redshift
        conn = redshift_utils.get_redshift_connection(host, port, database, user, password)
        
        try:
            # Get batch ID
            batch_id = get_batch_id(conn, context.batch_subject_area_id)
            context.batch_id = batch_id
            
            # Insert batch audit detail record
            insert_batch_audit_detail(
                conn,
                batch_id,
                context.parameter_workflow_name,
                context.batch_src_tbl_name,
                context.batch_tgt_tbl_name
            )
            
            logger.info("Main job tasks completed successfully")
            
            return {
                "batch_id": batch_id
            }
        finally:
            # Close the connection
            conn.close()
    except Exception as e:
        logger.error(f"Main job tasks failed: {str(e)}")
        raise