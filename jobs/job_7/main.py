#!/usr/bin/env python3
"""
Main AWS Glue Python Shell job script for Job_EDW_ST_CNSMPTN_CMN_DIM_US_STG_TO_SUBS_DTL.

This script serves as the entry point for the AWS Glue Python Shell job.
It handles parameter parsing, validation, logging setup, and exception handling.
"""

#dsads
import sys
import os
import uuid
from typing import Dict, Any

# Import utility modules
from src.utils import logging_utils
from src.utils import argument_utils

# Import job modules
from src.jobs.Job_EDW_ST_CNSMPTN_CMN_DIM_US_STG_TO_SUBS_DTL.pre_job import run_pre_job
from src.jobs.Job_EDW_ST_CNSMPTN_CMN_DIM_US_STG_TO_SUBS_DTL.main_job import run_main_job
from src.jobs.Job_EDW_ST_CNSMPTN_CMN_DIM_US_STG_TO_SUBS_DTL.post_job import run_post_job

# Import context
from src.context import context

# Import config
from src.config.config_loader import load_config

def main():
    """
    Main entry point for the Glue job.
    """
    # Set up logging
    logger = logging_utils.setup_logging()
    
    # Load environment-specific configuration from SSM
    try:
        config = load_config()
        environment_name = config.get('environment', {}).get('name', 'unknown')
        logger.info(f"Loaded configuration for environment: {environment_name}")
        
    except Exception as e:
        logger.error(f"Failed to load configuration: {str(e)}")
        logger.info("Continuing with default configuration")
        config = {}
    
    params = {}  # Initialize params outside try block for exception handler
    success = False
    pre_job_results = {}
    main_job_results = {}
    
    try:
        # Get job parameters
        params = argument_utils.get_default_job_params()
        
        # Validate parameters
        argument_utils.validate_job_params(
            params,
            required_params=['JOB_NAME']
        )
        
        # Set job name in context
        context.parameter_workflow_name = params.get('JOB_NAME', 'Job_EDW_ST_CNSMPTN_CMN_DIM_US_STG_TO_SUBS_DTL')
        context.batch_job_name = context.parameter_workflow_name
        
        # Log job start with parameters
        logging_utils.log_job_start(logger, context.parameter_workflow_name, params)
        
        # Run pre-job tasks
        logging_utils.log_step_start(logger, "pre_job_tasks")
        pre_job_results = run_pre_job(config)
        logging_utils.log_step_end(logger, "pre_job_tasks")
        
        # Run main job processing
        logging_utils.log_step_start(logger, "main_processing")
        main_job_results = run_main_job(pre_job_results, config)
        logging_utils.log_step_end(logger, "main_processing")
        
        # Run post-job tasks
        logging_utils.log_step_start(logger, "post_job_tasks")
        run_post_job(pre_job_results, main_job_results, config)
        logging_utils.log_step_end(logger, "post_job_tasks")
        
        # Mark as successful
        success = True
        
        # Log job end
        logging_utils.log_job_end(logger, context.parameter_workflow_name, success)
            
    except Exception as e:
        # Log the exception with traceback
        logging_utils.log_exception(logger, e)
        
        # Try to run post-job tasks to clean up resources
        try:
            if pre_job_results:
                logging_utils.log_step_start(logger, "error_cleanup")
                run_post_job(pre_job_results, main_job_results, config)
                logging_utils.log_step_end(logger, "error_cleanup")
        except Exception as cleanup_error:
            logger.error(f"Error during cleanup: {str(cleanup_error)}")
        
        # Log job end with failure
        job_name = context.parameter_workflow_name
        logging_utils.log_job_end(logger, job_name, False)
        
        # Exit with error code
        sys.exit(1)
    
    # Exit with appropriate code
    if not success:
        sys.exit(1)

if __name__ == "__main__":
    main()