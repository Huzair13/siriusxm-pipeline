"""
jl_Frmwrk_EDW_LOAD_CONTEXT joblet implementation.

This module provides functionality to load context variables from configuration files.
"""
import os
import logging
import json
from typing import Dict, Any, Optional

from src.context import context

logger = logging.getLogger(__name__)

def load_context_from_file(file_path: str) -> Dict[str, Any]:
    """
    Load context variables from a configuration file.
    
    Args:
        file_path: Path to the configuration file
        
    Returns:
        Dictionary of context variables
        
    Raises:
        Exception: If the file cannot be read or parsed
    """
    try:
        if not os.path.exists(file_path):
            logger.error(f"Context file not found: {file_path}")
            raise FileNotFoundError(f"Context file not found: {file_path}")
        
        variables = {}
        with open(file_path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                
                if '=' in line:
                    key, value = line.split('=', 1)
                    variables[key.strip()] = value.strip()
        
        logger.info(f"Loaded {len(variables)} context variables from {file_path}")
        return variables
    except Exception as e:
        logger.error(f"Failed to load context from {file_path}: {str(e)}")
        raise

def run_joblet(context_file_path: Optional[str] = None) -> None:
    """
    Run the jl_Frmwrk_EDW_LOAD_CONTEXT joblet.
    
    Args:
        context_file_path: Path to the context file (optional)
        
    Raises:
        Exception: If the context variables cannot be loaded
    """
    try:
        # If context_file_path is not provided, use the path from context variables
        if not context_file_path:
            context_file_path = os.path.join(
                context.parameter_file_local_context_path,
                context.parameter_file_context_file_nm
            )
        
        # Load context variables from file
        variables = load_context_from_file(context_file_path)
        
        # Update context with loaded variables
        context.update(variables)
        
        logger.info("Successfully loaded context variables")
    except Exception as e:
        logger.error(f"Failed to load context variables: {str(e)}")
        raise