"""
Pre-job tasks for the Job_Frmwrk_EDW_BATCH_DETAIL_START job.

This module contains functions for the pre-job phase of the Job_Frmwrk_EDW_BATCH_DETAIL_START job.
"""
import logging
from typing import Dict, Any, Optional

from src.context import context
from src.utils import redshift_utils, ssm_utils
from src.joblets.jl_Frmwrk_EDW_LOAD_CONTEXT import main_joblet as load_context_joblet

logger = logging.getLogger(__name__)

def run_pre_job(config: Dict[str, Any]) -> None:
    """
    Run pre-job tasks for the Job_Frmwrk_EDW_BATCH_DETAIL_START job.
    
    Args:
        config: Configuration dictionary
        
    Raises:
        Exception: If pre-job tasks fail
    """
    try:
        # Load context variables
        load_context_joblet.run_joblet()
        
        logger.info("Pre-job tasks completed successfully")
    except Exception as e:
        logger.error(f"Pre-job tasks failed: {str(e)}")
        raise