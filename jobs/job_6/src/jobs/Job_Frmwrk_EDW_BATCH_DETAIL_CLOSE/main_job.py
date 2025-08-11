"""
Main job tasks for the Job_Frmwrk_EDW_BATCH_DETAIL_CLOSE job.

This module contains functions for the main processing phase of the Job_Frmwrk_EDW_BATCH_DETAIL_CLOSE job.
"""
import logging
from typing import Dict, Any, Optional

from src.context import context
from src.utils import redshift_utils, ssm_utils

logger = logging.getLogger(__name__)

def get_batch_detail_id(conn, job_name: str) -> int:
    """
    Get the batch detail ID for the job.
    
    Args:
        conn: Redshift connection
        job_name: Job name
        
    Returns:
        Batch detail ID
        
    Raises:
        Exception: If the batch detail ID cannot be retrieved
    """
    try:
        query = f"""
        select max(batch_detail_id) from edw_ods.batch_audit_detail 
        where JOB_NM = '{job_name}' 
        and PROCESS_STATUS_CD = 'In Progress'
        """
        
        result = redshift_utils.fetch_one(conn, query)
        
        if result and result[0]:
            batch_detail_id = result[0]
            logger.info(f"Retrieved batch detail ID: {batch_detail_id}")
            return batch_detail_id
        else:
            logger.error("No batch detail ID found")
            raise ValueError("No batch detail ID found")
    except Exception as e:
        logger.error(f"Failed to get batch detail ID: {str(e)}")
        raise

def update_batch_audit_detail(conn, batch_detail_id: int, src_rec_qty: int, ins_rec_qty: int, 
                             upd_rec_qty: int, err_rec_qty: int, file_nm: str = "N/A", 
                             file_rcvd_ts: Optional[str] = None, file_size: Optional[int] = None) -> None:
    """
    Update the batch audit detail record.
    
    Args:
        conn: Redshift connection
        batch_detail_id: Batch detail ID
        src_rec_qty: Source record count
        ins_rec_qty: Inserted record count
        upd_rec_qty: Updated record count
        err_rec_qty: Error record count
        file_nm: File name (optional)
        file_rcvd_ts: File received timestamp (optional)
        file_size: File size in bytes (optional)
        
    Raises:
        Exception: If the record cannot be updated
    """
    try:
        # Handle optional parameters
        file_nm_value = f"'{file_nm}'" if file_nm else "'N/A'"
        file_rcvd_ts_value = f"'{file_rcvd_ts}'" if file_rcvd_ts else "null"
        file_size_value = str(file_size) if file_size is not None else "null"
        
        query = f"""
        begin;
        UPDATE edw_ods.batch_audit_detail 
        SET 
        PROCESS_STATUS_CD = 'Complete', 
        batch_detail_end_ts = current_timestamp,
        FILE_NM = {file_nm_value},
        FILE_RCVD_TS = {file_rcvd_ts_value},
        FILE_SIZE_IN_BYTES_QTY = {file_size_value},
        SRC_REC_QTY = {src_rec_qty},
        INS_REC_QTY = {ins_rec_qty},
        UPD_REC_QTY = {upd_rec_qty},
        ERR_REC_QTY = {err_rec_qty}
        WHERE 
        batch_detail_id = {batch_detail_id};
        end;
        """
        
        redshift_utils.execute_query(conn, query)
        logger.info(f"Updated batch audit detail record for batch detail ID {batch_detail_id}")
    except Exception as e:
        logger.error(f"Failed to update batch audit detail record: {str(e)}")
        raise

def run_main_job(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Run main job tasks for the Job_Frmwrk_EDW_BATCH_DETAIL_CLOSE job.
    
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
            # Get batch detail ID
            batch_detail_id = get_batch_detail_id(conn, context.parameter_workflow_name)
            context.batch_detail_id = batch_detail_id
            
            # Update batch audit detail record
            update_batch_audit_detail(
                conn,
                batch_detail_id,
                context.batch_audit_detail_SRC_REC_QTY,
                context.batch_audit_detail_INS_REC_QTY,
                context.batch_audit_detail_UPD_REC_QTY,
                context.batch_audit_detail_ERR_REC_QTY,
                context.batch_audit_detail_FILE_NM,
                context.batch_audit_detail_FILE_RCVD_TS,
                context.batch_audit_detail_FILE_SIZE_IN_BYTES_QTY
            )
            
            logger.info("Main job tasks completed successfully")
            
            return {
                "batch_detail_id": batch_detail_id
            }
        finally:
            # Close the connection
            conn.close()
    except Exception as e:
        logger.error(f"Main job tasks failed: {str(e)}")
        raise