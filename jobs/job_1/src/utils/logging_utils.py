"""
Logging utility functions for AWS Glue Python Shell jobs.

This module provides enhanced logging functionality specifically designed for AWS Glue jobs,
ensuring logs are properly captured in CloudWatch Logs while following best practices.
"""
import logging
import sys
import traceback
import threading
from typing import Optional, Dict, Any, Union, List

# Removed thread-local storage for job context

def setup_logging(
    log_level: Union[int, str] = logging.INFO,
    log_format: Optional[str] = None,
    date_format: Optional[str] = None
) -> logging.Logger:
    """
    Set up logging for the AWS Glue job with CloudWatch compatibility.
    
    Args:
        log_level: Logging level (default: INFO)
        log_format: Log format string (optional)
        date_format: Date format string (optional)
        
    Returns:
        Logger instance
    """
    if log_format is None:
        # CloudWatch already adds timestamps, so we don't need to include them
        # A simpler format focused on level and message is better for CloudWatch
        log_format = '[%(levelname)s] %(message)s'
    
    # Date format is not needed if we're not using asctime in the format
    date_format = None
    
    # Convert string log level to numeric if needed
    if isinstance(log_level, str):
        log_level = getattr(logging, log_level.upper(), logging.INFO)
    
    # Reset the root logger completely
    root = logging.getLogger()
    if root.handlers:
        for handler in root.handlers:
            root.removeHandler(handler)
    
    # Standard approach: Use StreamHandler with sys.stdout
    # AWS Glue automatically captures stdout and sends it to CloudWatch Logs
    handler = logging.StreamHandler(sys.stdout)
    
    # Create a formatter with the specified format
    formatter = logging.Formatter(log_format, date_format)
    handler.setFormatter(formatter)
    
    # Set the log level for the handler
    handler.setLevel(log_level)
    
    # Add the handler to the root logger
    root.setLevel(log_level)
    root.addHandler(handler)
    
    # Suppress AWS SDK verbose logging
    for logger_name in ['boto3', 'botocore', 's3transfer', 'urllib3', 'matplotlib']:
        logging.getLogger(logger_name).setLevel(logging.WARNING)
    
    # Log initialization
    root.info("Logging initialized for AWS Glue job")
    
    return root

def get_logger(name: str, log_level: Optional[Union[int, str]] = None) -> logging.Logger:
    """
    Get a logger with the specified name.
    
    Args:
        name: Logger name
        log_level: Optional log level override
        
    Returns:
        Logger instance
    """
    logger = logging.getLogger(name)
    
    if log_level is not None:
        # Convert string log level to numeric if needed
        if isinstance(log_level, str):
            log_level = getattr(logging, log_level.upper(), logging.INFO)
        logger.setLevel(log_level)
    
    return logger

def log_job_start(logger: logging.Logger, job_name: str, job_args: Dict[str, Any]) -> None:
    """
    Log job start with parameters.
    
    Args:
        logger: Logger instance
        job_name: Name of the job
        job_args: Job arguments
    """
    logger.info(f"Starting job: {job_name}")
    logger.info(f"Job parameters: {job_args}")

def log_job_end(logger: logging.Logger, job_name: str, success: bool = True) -> None:
    """
    Log job end with status.
    
    Args:
        logger: Logger instance
        job_name: Name of the job
        success: Whether the job was successful
    """
    status = "successfully" if success else "with errors"
    logger.info(f"Job {job_name} completed {status}")

def log_step_start(logger: logging.Logger, step_name: str) -> None:
    """
    Log the start of a processing step.
    
    Args:
        logger: Logger instance
        step_name: Name of the step
    """
    logger.info(f"Starting step: {step_name}")

def log_step_end(logger: logging.Logger, step_name: str, success: bool = True) -> None:
    """
    Log the end of a processing step.
    
    Args:
        logger: Logger instance
        step_name: Name of the step
        success: Whether the step was successful
    """
    status = "successfully" if success else "with errors"
    logger.info(f"Step {step_name} completed {status}")

def log_exception(logger: logging.Logger, exception: Exception, include_traceback: bool = True) -> None:
    """
    Log an exception with optional traceback.
    
    Args:
        logger: Logger instance
        exception: The exception to log
        include_traceback: Whether to include the traceback
    """
    if include_traceback:
        tb_lines = traceback.format_exception(type(exception), exception, exception.__traceback__)
        logger.error(f"Exception: {str(exception)}\nTraceback:\n{''.join(tb_lines)}")
    else:
        logger.error(f"Exception: {str(exception)}")

def log_dict(logger: logging.Logger, title: str, data: Dict[str, Any], level: int = logging.INFO) -> None:
    """
    Log a dictionary with a title.
    
    Args:
        logger: Logger instance
        title: Title for the log entry
        data: Dictionary to log
        level: Log level to use
    """
    if logger.isEnabledFor(level):
        logger.log(level, f"{title}:")
        for key, value in data.items():
            logger.log(level, f"  {key}: {value}")

# log_metrics function removed as it's not needed
