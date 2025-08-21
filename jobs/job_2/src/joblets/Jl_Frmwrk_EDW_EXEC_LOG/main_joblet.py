"""
Jl_Frmwrk_EDW_EXEC_LOG joblet implementation.

This module provides functionality for execution logging and statistics.
"""
import os
import logging
import datetime
from typing import Dict, Any, Optional

from src.context import context
from src.utils import logging_utils

logger = logging.getLogger(__name__)

def log_statistics(log_file_path: str, statistics: Dict[str, Any]) -> None:
    """
    Log execution statistics to a file.
    
    Args:
        log_file_path: Path to the log file
        statistics: Dictionary of statistics to log
        
    Raises:
        Exception: If the statistics cannot be logged
    """
    try:
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
        
        # Format the statistics as a pipe-delimited string
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        stats_line = f"{timestamp}|{statistics.get('job_name', 'unknown')}|{statistics.get('duration', 0)}|"
        stats_line += f"{statistics.get('status', 'unknown')}|{statistics.get('records_processed', 0)}|"
        stats_line += f"{statistics.get('father_pid', '0')}|{statistics.get('pid', '0')}\n"
        
        # Write to the log file
        with open(log_file_path, 'a') as f:
            f.write(stats_line)
        
        logger.info(f"Logged statistics to {log_file_path}")
    except Exception as e:
        logger.error(f"Failed to log statistics to {log_file_path}: {str(e)}")
        # Don't raise the exception, as this is a non-critical operation

def log_event(log_file_path: str, event: Dict[str, Any]) -> None:
    """
    Log an event to a file.
    
    Args:
        log_file_path: Path to the log file
        event: Dictionary of event information to log
        
    Raises:
        Exception: If the event cannot be logged
    """
    try:
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
        
        # Format the event as a pipe-delimited string
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        event_line = f"{timestamp}|{event.get('job_name', 'unknown')}|{event.get('level', 'INFO')}|"
        event_line += f"{event.get('message', '')}|{event.get('father_pid', '0')}|{event.get('pid', '0')}\n"
        
        # Write to the log file
        with open(log_file_path, 'a') as f:
            f.write(event_line)
        
        logger.info(f"Logged event to {log_file_path}")
    except Exception as e:
        logger.error(f"Failed to log event to {log_file_path}: {str(e)}")
        # Don't raise the exception, as this is a non-critical operation

def run_joblet(job_name: Optional[str] = None) -> None:
    """
    Run the Jl_Frmwrk_EDW_EXEC_LOG joblet.
    
    Args:
        job_name: Name of the job (optional)
    """
    try:
        # If job_name is not provided, use the name from context variables
        if not job_name:
            job_name = context.batch_job_name
        
        # Create log file paths
        status_file_path = os.path.join(
            context.ic_etl_local_home_path_nm,
            "log",
            context.file_status_file_nm.replace("jobname", job_name)
        )
        
        log_file_path = os.path.join(
            context.ic_etl_local_home_path_nm,
            "log",
            context.file_log_file_nm.replace("jobname", job_name)
        )
        
        # Log initial statistics
        statistics = {
            "job_name": job_name,
            "duration": 0,
            "status": "In Progress",
            "records_processed": 0,
            "father_pid": context.father_pid,
            "pid": context.pid
        }
        log_statistics(status_file_path, statistics)
        
        # Log initial event
        event = {
            "job_name": job_name,
            "level": "INFO",
            "message": f"Job {job_name} started",
            "father_pid": context.father_pid,
            "pid": context.pid
        }
        log_event(log_file_path, event)
        
        logger.info(f"Initialized execution logging for job {job_name}")
    except Exception as e:
        logger.error(f"Failed to initialize execution logging: {str(e)}")
        # Don't raise the exception, as this is a non-critical operation