"""
Argument parsing utility functions for AWS Glue Python Shell jobs.
"""
import argparse
from typing import Dict, Any, List, Optional, Union

#sasdaw
def get_job_arguments(
    required_args: Optional[List[str]] = None,
    optional_args: Optional[Dict[str, Any]] = None,
    description: str = "AWS Glue Python Shell Job"
) -> argparse.Namespace:
    """
    Parse job arguments from command line.
    
    Args:
        required_args: List of required argument names
        optional_args: Dictionary of optional arguments with default values
        description: Job description for help text
        
    Returns:
        Namespace containing the parsed arguments
    """
    parser = argparse.ArgumentParser(description=description)
    
    # Add required arguments
    if required_args:
        for arg_name in required_args:
            parser.add_argument(
                f"--{arg_name}",
                required=True,
                help=f"Required: {arg_name}"
            )
    
    # Add optional arguments with defaults
    if optional_args:
        for arg_name, default_value in optional_args.items():
            help_text = f"Optional: {arg_name}"
            if default_value is not None:
                help_text += f" (default: {default_value})"
                
            parser.add_argument(
                f"--{arg_name}",
                default=default_value,
                required=False,
                help=help_text
            )
    
    # Parse known args to handle AWS Glue's additional arguments
    args, _ = parser.parse_known_args()
    return args

def get_job_params(
    required_params: Optional[List[str]] = None,
    optional_params: Optional[Dict[str, Any]] = None,
    description: str = "AWS Glue Python Shell Job"
) -> Dict[str, Any]:
    """
    Parse job arguments and return as a dictionary.
    
    Args:
        required_params: List of required parameter names
        optional_params: Dictionary of optional parameters with default values
        description: Job description for help text
        
    Returns:
        Dictionary containing the parsed parameters
    """
    args = get_job_arguments(required_params, optional_params, description)
    return vars(args)

def validate_job_params(
    params: Dict[str, Any],
    required_params: List[str],
    param_validators: Optional[Dict[str, callable]] = None
) -> bool:
    """
    Validate job parameters.
    
    Args:
        params: Dictionary of parameters
        required_params: List of required parameter names
        param_validators: Dictionary mapping parameter names to validator functions
        
    Returns:
        True if all validations pass
        
    Raises:
        ValueError: If validation fails
    """
    # Check required parameters
    for param in required_params:
        if param not in params or params[param] is None:
            raise ValueError(f"Required parameter '{param}' is missing")
    
    # Run custom validators
    if param_validators:
        for param, validator in param_validators.items():
            if param in params and params[param] is not None:
                if not validator(params[param]):
                    raise ValueError(f"Parameter '{param}' failed validation")
    
    return True

def s3_path_validator(path: str) -> bool:
    """
    Validate that a string is a valid S3 path.
    
    Args:
        path: S3 path to validate
        
    Returns:
        True if valid, False otherwise
    """
    if not isinstance(path, str):
        return False
    
    if not path.startswith('s3://'):
        return False
    
    parts = path[5:].split('/', 1)
    if len(parts) < 1 or not parts[0]:
        return False
    
    return True

def get_default_job_params() -> Dict[str, Any]:
    """
    Get default job parameters for a standard Glue Python Shell job.
    
    Returns:
        Dictionary of parsed parameters
    """
    required_params = ['JOB_NAME']
    
    optional_params = {
        'log_level': 'INFO',
        'region': None,
    }
    
    return get_job_params(
        required_params=required_params,
        optional_params=optional_params,
        description="AWS Glue Python Shell Job"
    )
