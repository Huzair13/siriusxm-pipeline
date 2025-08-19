"""
Post-job tasks for the Job_Frmwrk_EDW_BATCH_DETAIL_CLOSE job.

This module contains functions for the post-job phase of the Job_Frmwrk_EDW_BATCH_DETAIL_CLOSE job.
"""
import logging
from typing import Dict, Any, Optional

from src.context import context

logger = logging.getLogger(__name__)

def run_post_job(job_results: Dict[str, Any], config: Dict[str, Any]) -> None:
    """
    Run post-job tasks for the Job_Frmwrk_EDW_BATCH_DETAIL_CLOSE job.
    
    Args:
        job_results: Dictionary of job results
        config: Configuration dictionary
        
    Raises:
        Exception: If post-job tasks fail
    """
    try:
        # No specific post-job tasks for this job
        logger.info("Post-job tasks completed successfully")
    except Exception as e:
        logger.error(f"Post-job tasks failed: {str(e)}")
        raise