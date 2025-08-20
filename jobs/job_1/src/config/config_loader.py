"""
Configuration loader for AWS Glue Python Shell jobs.

This module provides functionality to load environment-specific configurations
from INI files based on the environment retrieved from AWS SSM Parameter Store.
"""
import logging
import configparser
import os
from typing import Dict, Any

from src.utils import ssm_utils

logger = logging.getLogger(__name__)

def get_environment_from_ssm(parameter_name: str = "talend/migration/env", region_name: str = None) -> str:
    """
    Get the current environment from AWS SSM Parameter Store.
    
    Args:
        parameter_name: SSM parameter name containing the environment
        region_name: AWS region name (optional)
        
    Returns:
        str: Environment name (test or prod)
        
    Raises:
        Exception: If unable to retrieve environment from SSM
    """
    try:
        environment = ssm_utils.get_parameter(parameter_name, False, region_name)
        environment = environment.lower().strip()
        
        if environment not in ['test', 'prod']:
            logger.warning(f"Invalid environment '{environment}' from SSM, defaulting to 'test'")
            environment = 'test'
            
        logger.info(f"Retrieved environment '{environment}' from SSM parameter '{parameter_name}'")
        return environment
        
    except Exception as e:
        logger.error(f"Failed to retrieve environment from SSM parameter '{parameter_name}': {str(e)}")
        logger.info("Defaulting to 'test' environment")
        return 'test'

def load_ini_config(config_file_path: str) -> Dict[str, Any]:
    """
    Load configuration from an INI file and convert to dictionary.
    
    Args:
        config_file_path: Path to the INI configuration file
        
    Returns:
        Dict[str, Any]: Configuration as dictionary
        
    Raises:
        Exception: If unable to load configuration file
    """
    try:
        config = configparser.ConfigParser()
        config.read(config_file_path)
        
        # Convert INI sections to nested dictionary
        result = {}
        for section in config.sections():
            section_dict = {}
            for key, value in config.items(section):
                # Try to convert to int if possible
                try:
                    section_dict[key] = int(value)
                except ValueError:
                    # Keep as string if not an integer
                    section_dict[key] = value
            result[section] = section_dict
        
        logger.info(f"Loaded configuration from {config_file_path}")
        return result
        
    except Exception as e:
        logger.error(f"Failed to load configuration from {config_file_path}: {str(e)}")
        raise

def load_config(ssm_parameter_name: str = "talend/migration/env", region_name: str = None) -> Dict[str, Any]:
    """
    Load the appropriate configuration based on the environment from SSM.
    
    Args:
        ssm_parameter_name: SSM parameter name containing the environment
        region_name: AWS region name (optional)
        
    Returns:
        Dict[str, Any]: Environment-specific configuration
        
    Raises:
        Exception: If unable to load configuration
    """
    try:
        # Get environment from SSM
        environment = get_environment_from_ssm(ssm_parameter_name, region_name)
        
        # Get the directory where this module is located
        current_dir = os.path.dirname(os.path.abspath(__file__))
        
        # Load the appropriate INI file based on environment
        if environment == 'prod':
            config_file_path = os.path.join(current_dir, 'prod_config.ini')
            logger.info("Loading production configuration from INI file")
        else:
            config_file_path = os.path.join(current_dir, 'test_config.ini')
            logger.info("Loading test configuration from INI file")
        
        # Load configuration from INI file
        config = load_ini_config(config_file_path)
        
        return config
            
    except Exception as e:
        logger.error(f"Failed to load configuration: {str(e)}")
        raise 