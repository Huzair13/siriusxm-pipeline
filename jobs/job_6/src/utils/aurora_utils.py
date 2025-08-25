"""
Aurora PostgreSQL database utility functions for AWS Glue Python Shell jobs.
"""
import boto3
import logging
import json
from typing import Dict, Any, List, Optional, Union, Tuple
import pandas as pd

# Import database drivers
try:
    import psycopg2
    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False

logger = logging.getLogger(__name__)

def get_aurora_postgres_connection(
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
    **kwargs
) -> psycopg2.extensions.connection:
    """
    Get a connection to an Aurora PostgreSQL database.
    
    Args:
        host: Aurora PostgreSQL host
        port: Aurora PostgreSQL port
        database: Aurora PostgreSQL database name
        user: Aurora PostgreSQL user
        password: Aurora PostgreSQL password
        **kwargs: Additional connection parameters
        
    Returns:
        psycopg2 connection
        
    Raises:
        ImportError: If psycopg2 is not installed
        Exception: If the connection cannot be established
    """
    if not POSTGRES_AVAILABLE:
        raise ImportError("psycopg2 is not installed. Install it with 'pip install psycopg2-binary'")
    
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=database,
            user=user,
            password=password,
            **kwargs
        )
        logger.info(f"Successfully connected to Aurora PostgreSQL database: {database} on {host}")
        return conn
    except Exception as e:
        logger.error(f"Error connecting to Aurora PostgreSQL database: {str(e)}")
        raise

def get_aurora_postgres_connection_from_secret(
    secret_name: str,
    region_name: Optional[str] = None
) -> psycopg2.extensions.connection:
    """
    Get a connection to an Aurora PostgreSQL database using credentials from Secrets Manager.
    
    Args:
        secret_name: Name of the secret containing Aurora PostgreSQL credentials
        region_name: AWS region name (optional)
        
    Returns:
        psycopg2 connection
        
    Raises:
        ImportError: If psycopg2 is not installed
        Exception: If the connection cannot be established
    """
    if not POSTGRES_AVAILABLE:
        raise ImportError("psycopg2 is not installed. Install it with 'pip install psycopg2-binary'")
    
    try:
        # Import here to avoid circular imports
        from src.utils.ssm_utils import get_parameter
        
        # Get the secret
        secret_value = get_parameter(secret_name, True, region_name)
        
        # Parse the secret
        secret = json.loads(secret_value)
        
        # Get the connection
        return get_aurora_postgres_connection(
            host=secret['host'],
            port=secret.get('port', 5432),
            database=secret['database'],
            user=secret['username'],
            password=secret['password']
        )
    except Exception as e:
        logger.error(f"Error getting Aurora PostgreSQL connection from secret: {str(e)}")
        raise

def execute_query(
    conn: psycopg2.extensions.connection,
    query: str,
    params: Optional[Union[Tuple, Dict[str, Any]]] = None,
    commit: bool = True
) -> int:
    """
    Execute a query on an Aurora PostgreSQL database.
    
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
        
        logger.info(f"Successfully executed PostgreSQL query, {row_count} rows affected")
        return row_count
    except Exception as e:
        logger.error(f"Error executing PostgreSQL query: {str(e)}")
        if commit:
            conn.rollback()
        raise

def fetch_all(
    conn: psycopg2.extensions.connection,
    query: str,
    params: Optional[Union[Tuple, Dict[str, Any]]] = None
) -> List[Tuple]:
    """
    Fetch all rows from a query on an Aurora PostgreSQL database.
    
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
        
        logger.info(f"Successfully fetched {len(rows)} rows from PostgreSQL")
        return rows
    except Exception as e:
        logger.error(f"Error fetching rows from PostgreSQL: {str(e)}")
        raise

def fetch_one(
    conn: psycopg2.extensions.connection,
    query: str,
    params: Optional[Union[Tuple, Dict[str, Any]]] = None
) -> Optional[Tuple]:
    """
    Fetch one row from a query on an Aurora PostgreSQL database.
    
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
    Fetch query results as a pandas DataFrame from Aurora PostgreSQL.
    
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
        
        logger.info(f"Successfully fetched {len(df)} rows as DataFrame from PostgreSQL")
        return df
    except Exception as e:
        logger.error(f"Error fetching DataFrame from PostgreSQL: {str(e)}")
        raise
