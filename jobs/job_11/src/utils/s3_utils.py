"""
S3 utility functions for AWS Glue Python Shell jobs.
"""
import boto3
import logging
from typing import Tuple, Optional, Dict, Any, BinaryIO, Union
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

def parse_s3_path(s3_path: str) -> Tuple[str, str]:
    """
    Parse S3 path into bucket and key.
    
    Args:
        s3_path: S3 path in the format s3://bucket-name/key/path
        
    Returns:
        Tuple containing (bucket_name, key)
    """
    if s3_path.startswith('s3://'):
        s3_path = s3_path[5:]
    
    parts = s3_path.split('/', 1)
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ''
    
    return bucket, key

def get_s3_client(region_name: Optional[str] = None) -> boto3.client:
    """
    Get an S3 client.
    
    Args:
        region_name: AWS region name (optional)
        
    Returns:
        boto3 S3 client
    """
    return boto3.client('s3', region_name=region_name)

def get_s3_object(bucket: str, key: str, region_name: Optional[str] = None) -> Dict[str, Any]:
    """
    Get an object from S3.
    
    Args:
        bucket: S3 bucket name
        key: S3 object key
        region_name: AWS region name (optional)
        
    Returns:
        S3 object response
        
    Raises:
        ClientError: If the object cannot be retrieved
    """
    try:
        s3_client = get_s3_client(region_name)
        response = s3_client.get_object(Bucket=bucket, Key=key)
        logger.info(f"Successfully retrieved object from s3://{bucket}/{key}")
        return response
    except ClientError as e:
        logger.error(f"Error getting object from S3: {str(e)}")
        raise

def read_s3_file_to_string(bucket: str, key: str, encoding: str = 'utf-8', region_name: Optional[str] = None) -> str:
    """
    Read an S3 file to a string.
    
    Args:
        bucket: S3 bucket name
        key: S3 object key
        encoding: File encoding (default: utf-8)
        region_name: AWS region name (optional)
        
    Returns:
        File content as string
        
    Raises:
        ClientError: If the file cannot be read
    """
    response = get_s3_object(bucket, key, region_name)
    return response['Body'].read().decode(encoding)

def put_s3_object(bucket: str, key: str, body: Union[str, bytes, BinaryIO], 
                  content_type: Optional[str] = None, region_name: Optional[str] = None) -> Dict[str, Any]:
    """
    Put an object in S3.
    
    Args:
        bucket: S3 bucket name
        key: S3 object key
        body: Object content
        content_type: Content type (optional)
        region_name: AWS region name (optional)
        
    Returns:
        S3 put_object response
        
    Raises:
        ClientError: If the object cannot be put
    """
    try:
        s3_client = get_s3_client(region_name)
        
        # Build put_object kwargs
        put_kwargs = {'Bucket': bucket, 'Key': key, 'Body': body}
        if content_type:
            put_kwargs['ContentType'] = content_type
            
        response = s3_client.put_object(**put_kwargs)
        logger.info(f"Successfully uploaded object to s3://{bucket}/{key}")
        return response
    except ClientError as e:
        logger.error(f"Error putting object to S3: {str(e)}")
        raise

def list_s3_objects(bucket: str, prefix: str = '', region_name: Optional[str] = None) -> Dict[str, Any]:
    """
    List objects in an S3 bucket with a given prefix.
    
    Args:
        bucket: S3 bucket name
        prefix: S3 prefix (optional)
        region_name: AWS region name (optional)
        
    Returns:
        S3 list_objects_v2 response
        
    Raises:
        ClientError: If the objects cannot be listed
    """
    try:
        s3_client = get_s3_client(region_name)
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        logger.info(f"Successfully listed objects in s3://{bucket}/{prefix}")
        return response
    except ClientError as e:
        logger.error(f"Error listing objects in S3: {str(e)}")
        raise

def copy_s3_object(source_bucket: str, source_key: str, 
                  dest_bucket: str, dest_key: str, 
                  region_name: Optional[str] = None) -> Dict[str, Any]:
    """
    Copy an object within S3.
    
    Args:
        source_bucket: Source S3 bucket name
        source_key: Source S3 object key
        dest_bucket: Destination S3 bucket name
        dest_key: Destination S3 object key
        region_name: AWS region name (optional)
        
    Returns:
        S3 copy_object response
        
    Raises:
        ClientError: If the object cannot be copied
    """
    try:
        s3_client = get_s3_client(region_name)
        response = s3_client.copy_object(
            CopySource={'Bucket': source_bucket, 'Key': source_key},
            Bucket=dest_bucket,
            Key=dest_key
        )
        logger.info(f"Successfully copied s3://{source_bucket}/{source_key} to s3://{dest_bucket}/{dest_key}")
        return response
    except ClientError as e:
        logger.error(f"Error copying object in S3: {str(e)}")
        raise

def delete_s3_object(bucket: str, key: str, region_name: Optional[str] = None) -> Dict[str, Any]:
    """
    Delete an object from S3.
    
    Args:
        bucket: S3 bucket name
        key: S3 object key
        region_name: AWS region name (optional)
        
    Returns:
        S3 delete_object response
        
    Raises:
        ClientError: If the object cannot be deleted
    """
    try:
        s3_client = get_s3_client(region_name)
        response = s3_client.delete_object(Bucket=bucket, Key=key)
        logger.info(f"Successfully deleted object s3://{bucket}/{key}")
        return response
    except ClientError as e:
        logger.error(f"Error deleting object from S3: {str(e)}")
        raise
