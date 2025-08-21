"""
SSM Parameter Store utility functions for AWS Glue Python Shell jobs.
"""
import boto3
import logging
from typing import Dict, Any, List, Optional, Union

logger = logging.getLogger(__name__)

def get_ssm_client(region_name: Optional[str] = None) -> boto3.client:
    """
    Get an SSM client.
    
    Args:
        region_name: AWS region name (optional)
        
    Returns:
        boto3 SSM client
    """
    return boto3.client('ssm', region_name=region_name)

def get_parameter(name: str, with_decryption: bool = True, region_name: Optional[str] = None) -> str:
    """
    Get a parameter from SSM Parameter Store.
    
    Args:
        name: Parameter name
        with_decryption: Whether to decrypt the parameter (default: True)
        region_name: AWS region name (optional)
        
    Returns:
        Parameter value
        
    Raises:
        Exception: If the parameter cannot be retrieved
    """
    try:
        ssm_client = get_ssm_client(region_name)
        response = ssm_client.get_parameter(
            Name=name,
            WithDecryption=with_decryption
        )
        logger.info(f"Successfully retrieved parameter: {name}")
        return response['Parameter']['Value']
    except Exception as e:
        logger.error(f"Error getting parameter {name}: {str(e)}")
        raise

def get_parameters(names: List[str], with_decryption: bool = True, region_name: Optional[str] = None) -> Dict[str, str]:
    """
    Get multiple parameters from SSM Parameter Store.
    
    Args:
        names: List of parameter names
        with_decryption: Whether to decrypt the parameters (default: True)
        region_name: AWS region name (optional)
        
    Returns:
        Dictionary of parameter names and values
        
    Raises:
        Exception: If the parameters cannot be retrieved
    """
    try:
        ssm_client = get_ssm_client(region_name)
        response = ssm_client.get_parameters(
            Names=names,
            WithDecryption=with_decryption
        )
        
        # Check if any parameters were not found
        if response['InvalidParameters']:
            logger.warning(f"Invalid parameters: {response['InvalidParameters']}")
        
        # Create a dictionary of parameter names and values
        parameters = {param['Name']: param['Value'] for param in response['Parameters']}
        
        logger.info(f"Successfully retrieved {len(parameters)} parameters")
        return parameters
    except Exception as e:
        logger.error(f"Error getting parameters {names}: {str(e)}")
        raise

def get_parameters_by_path(path: str, recursive: bool = True, with_decryption: bool = True, 
                          region_name: Optional[str] = None) -> Dict[str, str]:
    """
    Get parameters by path from SSM Parameter Store.
    
    Args:
        path: Parameter path
        recursive: Whether to retrieve parameters recursively (default: True)
        with_decryption: Whether to decrypt the parameters (default: True)
        region_name: AWS region name (optional)
        
    Returns:
        Dictionary of parameter names and values
        
    Raises:
        Exception: If the parameters cannot be retrieved
    """
    try:
        ssm_client = get_ssm_client(region_name)
        parameters = {}
        
        # SSM get_parameters_by_path returns paginated results
        paginator = ssm_client.get_paginator('get_parameters_by_path')
        page_iterator = paginator.paginate(
            Path=path,
            Recursive=recursive,
            WithDecryption=with_decryption
        )
        
        # Process each page of results
        for page in page_iterator:
            for param in page['Parameters']:
                parameters[param['Name']] = param['Value']
        
        logger.info(f"Successfully retrieved {len(parameters)} parameters from path {path}")
        return parameters
    except Exception as e:
        logger.error(f"Error getting parameters from path {path}: {str(e)}")
        raise

def put_parameter(name: str, value: str, description: Optional[str] = None, 
                 param_type: str = 'String', overwrite: bool = True, 
                 region_name: Optional[str] = None) -> Dict[str, Any]:
    """
    Put a parameter in SSM Parameter Store.
    
    Args:
        name: Parameter name
        value: Parameter value
        description: Parameter description (optional)
        param_type: Parameter type (String, StringList, or SecureString) (default: String)
        overwrite: Whether to overwrite an existing parameter (default: True)
        region_name: AWS region name (optional)
        
    Returns:
        SSM put_parameter response
        
    Raises:
        Exception: If the parameter cannot be put
    """
    try:
        ssm_client = get_ssm_client(region_name)
        
        # Build put_parameter kwargs
        put_kwargs = {
            'Name': name,
            'Value': value,
            'Type': param_type,
            'Overwrite': overwrite
        }
        
        if description:
            put_kwargs['Description'] = description
            
        response = ssm_client.put_parameter(**put_kwargs)
        logger.info(f"Successfully put parameter: {name}")
        return response
    except Exception as e:
        logger.error(f"Error putting parameter {name}: {str(e)}")
        raise

def delete_parameter(name: str, region_name: Optional[str] = None) -> Dict[str, Any]:
    """
    Delete a parameter from SSM Parameter Store.
    
    Args:
        name: Parameter name
        region_name: AWS region name (optional)
        
    Returns:
        SSM delete_parameter response
        
    Raises:
        Exception: If the parameter cannot be deleted
    """
    try:
        ssm_client = get_ssm_client(region_name)
        response = ssm_client.delete_parameter(Name=name)
        logger.info(f"Successfully deleted parameter: {name}")
        return response
    except Exception as e:
        logger.error(f"Error deleting parameter {name}: {str(e)}")
        raise
