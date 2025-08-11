"""
Redshift database utility functions for AWS Glue Python Shell jobs.
"""
import boto3
import psycopg2
import logging
from typing import Dict, Any, List, Optional, Union, Tuple
import pandas as pd

logger = logging.getLogger(__name__)

def get_redshift_connection(
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
    **kwargs
) -> psycopg2.extensions.connection:
    """
    Get a connection to a Redshift database.
    
    Args:
        host: Redshift host
        port: Redshift port
        database: Redshift database name
        user: Redshift user
        password: Redshift password
        **kwargs: Additional connection parameters
        
    Returns:
        psycopg2 connection
        
    Raises:
        Exception: If the connection cannot be established
    """
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=database,
            user=user,
            password=password,
            **kwargs
        )
        logger.info(f"Successfully connected to Redshift database: {database} on {host}")
        return conn
    except Exception as e:
        logger.error(f"Error connecting to Redshift database: {str(e)}")
        raise

def get_redshift_connection_from_secret(
    secret_name: str,
    region_name: Optional[str] = None
) -> psycopg2.extensions.connection:
    """
    Get a connection to a Redshift database using credentials from Secrets Manager.
    
    Args:
        secret_name: Name of the secret containing Redshift credentials
        region_name: AWS region name (optional)
        
    Returns:
        psycopg2 connection
        
    Raises:
        Exception: If the connection cannot be established
    """
    try:
        # Import here to avoid circular imports
        from src.utils.ssm_utils import get_parameter
        
        # Get the secret
        secret_value = get_parameter(secret_name, True, region_name)
        
        # Parse the secret
        import json
        secret = json.loads(secret_value)
        
        # Get the connection
        return get_redshift_connection(
            host=secret['host'],
            port=secret.get('port', 5439),
            database=secret['database'],
            user=secret['username'],
            password=secret['password']
        )
    except Exception as e:
        logger.error(f"Error getting Redshift connection from secret: {str(e)}")
        raise

def execute_query(
    conn: psycopg2.extensions.connection,
    query: str,
    params: Optional[Union[Tuple, Dict[str, Any]]] = None,
    commit: bool = True
) -> int:
    """
    Execute a query on a Redshift database.
    
    Args:
        conn: psycopg2 connection
        query: SQL query
        params: Query parameters (optional)
        commit: Whether to commit the transaction (default: True)
        
    Returns:
        Number of rows affected
        
    Raises:
        Exception: If the query cannot be executed
    """
    try:
        cursor = conn.cursor()
        cursor.execute(query, params)
        
        if commit:
            conn.commit()
            
        row_count = cursor.rowcount
        cursor.close()
        
        logger.info(f"Successfully executed query, {row_count} rows affected")
        return row_count
    except Exception as e:
        logger.error(f"Error executing query: {str(e)}")
        if commit:
            conn.rollback()
        raise

def fetch_all(
    conn: psycopg2.extensions.connection,
    query: str,
    params: Optional[Union[Tuple, Dict[str, Any]]] = None
) -> List[Tuple]:
    """
    Fetch all rows from a query on a Redshift database.
    
    Args:
        conn: psycopg2 connection
        query: SQL query
        params: Query parameters (optional)
        
    Returns:
        List of rows
        
    Raises:
        Exception: If the query cannot be executed
    """
    try:
        cursor = conn.cursor()
        cursor.execute(query, params)
        
        rows = cursor.fetchall()
        cursor.close()
        
        logger.info(f"Successfully fetched {len(rows)} rows")
        return rows
    except Exception as e:
        logger.error(f"Error fetching rows: {str(e)}")
        raise

def fetch_one(
    conn: psycopg2.extensions.connection,
    query: str,
    params: Optional[Union[Tuple, Dict[str, Any]]] = None
) -> Optional[Tuple]:
    """
    Fetch one row from a query on a Redshift database.
    
    Args:
        conn: psycopg2 connection
        query: SQL query
        params: Query parameters (optional)
        
    Returns:
        Row or None if no rows are returned
        
    Raises:
        Exception: If the query cannot be executed
    """
    try:
        cursor = conn.cursor()
        cursor.execute(query, params)
        
        row = cursor.fetchone()
        cursor.close()
        
        if row:
            logger.info("Successfully fetched one row")
        else:
            logger.info("No rows returned")
            
        return row
    except Exception as e:
        logger.error(f"Error fetching row: {str(e)}")
        raise

def fetch_as_dataframe(
    conn: psycopg2.extensions.connection,
    query: str,
    params: Optional[Union[Tuple, Dict[str, Any]]] = None
) -> pd.DataFrame:
    """
    Fetch query results as a pandas DataFrame.
    
    Args:
        conn: psycopg2 connection
        query: SQL query
        params: Query parameters (optional)
        
    Returns:
        pandas DataFrame
        
    Raises:
        Exception: If the query cannot be executed
    """
    try:
        # Use pandas to execute the query and return a DataFrame
        df = pd.read_sql_query(query, conn, params=params)
        
        logger.info(f"Successfully fetched {len(df)} rows as DataFrame")
        return df
    except Exception as e:
        logger.error(f"Error fetching DataFrame: {str(e)}")
        raise

def copy_to_s3(
    conn: psycopg2.extensions.connection,
    query: str,
    s3_path: str,
    aws_access_key_id: str,
    aws_secret_access_key: str,
    options: Optional[str] = None
) -> int:
    """
    Copy data from Redshift to S3.
    
    Args:
        conn: psycopg2 connection
        query: SQL query to select data
        s3_path: S3 path to copy data to
        aws_access_key_id: AWS access key ID
        aws_secret_access_key: AWS secret access key
        options: Additional UNLOAD options (optional)
        
    Returns:
        Number of rows unloaded
        
    Raises:
        Exception: If the data cannot be copied
    """
    try:
        cursor = conn.cursor()
        
        # Build the UNLOAD command
        unload_command = f"""
        UNLOAD ('{query}')
        TO '{s3_path}'
        ACCESS_KEY_ID '{aws_access_key_id}'
        SECRET_ACCESS_KEY '{aws_secret_access_key}'
        """
        
        if options:
            unload_command += f" {options}"
        
        # Execute the UNLOAD command
        cursor.execute(unload_command)
        conn.commit()
        
        # Get the number of rows unloaded
        cursor.execute("SELECT pg_last_unload_count()")
        row_count = cursor.fetchone()[0]
        
        cursor.close()
        
        logger.info(f"Successfully unloaded {row_count} rows to {s3_path}")
        return row_count
    except Exception as e:
        logger.error(f"Error copying data to S3: {str(e)}")
        conn.rollback()
        raise

def copy_from_s3(
    conn: psycopg2.extensions.connection,
    table_name: str,
    s3_path: str,
    aws_access_key_id: str,
    aws_secret_access_key: str,
    options: Optional[str] = None
) -> int:
    """
    Copy data from S3 to Redshift.
    
    Args:
        conn: psycopg2 connection
        table_name: Redshift table name
        s3_path: S3 path to copy data from
        aws_access_key_id: AWS access key ID
        aws_secret_access_key: AWS secret access key
        options: Additional COPY options (optional)
        
    Returns:
        Number of rows loaded
        
    Raises:
        Exception: If the data cannot be copied
    """
    try:
        cursor = conn.cursor()
        
        # Build the COPY command
        copy_command = f"""
        COPY {table_name}
        FROM '{s3_path}'
        ACCESS_KEY_ID '{aws_access_key_id}'
        SECRET_ACCESS_KEY '{aws_secret_access_key}'
        """
        
        if options:
            copy_command += f" {options}"
        
        # Execute the COPY command
        cursor.execute(copy_command)
        conn.commit()
        
        row_count = cursor.rowcount
        cursor.close()
        
        logger.info(f"Successfully loaded {row_count} rows from {s3_path} to {table_name}")
        return row_count
    except Exception as e:
        logger.error(f"Error copying data from S3: {str(e)}")
        conn.rollback()
        raise
