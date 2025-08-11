"""
Teradata database utility functions for AWS Glue Python Shell jobs.
"""
import logging
from typing import Dict, Any, List, Optional, Union, Tuple
import pandas as pd
import json

logger = logging.getLogger(__name__)

# Check if teradatasql is available
try:
    import teradatasql
    TERADATA_AVAILABLE = True
except ImportError:
    TERADATA_AVAILABLE = False

def get_teradata_connection(
    host: str,
    username: str,
    password: str,
    database: Optional[str] = None,
    **kwargs
) -> 'teradatasql.TeradataConnection':
    """
    Get a connection to a Teradata database.
    
    Args:
        host: Teradata host
        username: Teradata username
        password: Teradata password
        database: Teradata database name (optional)
        **kwargs: Additional connection parameters
        
    Returns:
        teradatasql connection
        
    Raises:
        ImportError: If teradatasql is not installed
        Exception: If the connection cannot be established
    """
    if not TERADATA_AVAILABLE:
        raise ImportError("teradatasql is not installed. Install it with 'pip install teradatasql'")
    
    try:
        # Build connection parameters
        conn_params = {
            'host': host,
            'user': username,
            'password': password,
        }
        
        # Add database if provided
        if database:
            conn_params['database'] = database
            
        # Add additional parameters
        conn_params.update(kwargs)
        
        # Connect to Teradata
        conn = teradatasql.connect(**conn_params)
        
        logger.info(f"Successfully connected to Teradata database on {host}")
        return conn
    except Exception as e:
        logger.error(f"Error connecting to Teradata database: {str(e)}")
        raise

def get_teradata_connection_from_secret(
    secret_name: str,
    region_name: Optional[str] = None
) -> 'teradatasql.TeradataConnection':
    """
    Get a connection to a Teradata database using credentials from Secrets Manager.
    
    Args:
        secret_name: Name of the secret containing Teradata credentials
        region_name: AWS region name (optional)
        
    Returns:
        teradatasql connection
        
    Raises:
        ImportError: If teradatasql is not installed
        Exception: If the connection cannot be established
    """
    if not TERADATA_AVAILABLE:
        raise ImportError("teradatasql is not installed. Install it with 'pip install teradatasql'")
    
    try:
        # Import here to avoid circular imports
        from src.utils.ssm_utils import get_parameter
        
        # Get the secret
        secret_value = get_parameter(secret_name, True, region_name)
        
        # Parse the secret
        secret = json.loads(secret_value)
        
        # Get the connection
        return get_teradata_connection(
            host=secret['host'],
            username=secret['username'],
            password=secret['password'],
            database=secret.get('database')
        )
    except Exception as e:
        logger.error(f"Error getting Teradata connection from secret: {str(e)}")
        raise

def execute_query(
    conn: 'teradatasql.TeradataConnection',
    query: str,
    params: Optional[Union[Tuple, Dict[str, Any], List]] = None,
    commit: bool = True
) -> int:
    """
    Execute a query on a Teradata database.
    
    Args:
        conn: teradatasql connection
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
    conn: 'teradatasql.TeradataConnection',
    query: str,
    params: Optional[Union[Tuple, Dict[str, Any], List]] = None
) -> List[Tuple]:
    """
    Fetch all rows from a query on a Teradata database.
    
    Args:
        conn: teradatasql connection
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
    conn: 'teradatasql.TeradataConnection',
    query: str,
    params: Optional[Union[Tuple, Dict[str, Any], List]] = None
) -> Optional[Tuple]:
    """
    Fetch one row from a query on a Teradata database.
    
    Args:
        conn: teradatasql connection
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
    conn: 'teradatasql.TeradataConnection',
    query: str,
    params: Optional[Union[Tuple, Dict[str, Any], List]] = None
) -> pd.DataFrame:
    """
    Fetch query results as a pandas DataFrame.
    
    Args:
        conn: teradatasql connection
        query: SQL query
        params: Query parameters (optional)
        
    Returns:
        pandas DataFrame
        
    Raises:
        Exception: If the query cannot be executed
    """
    try:
        # Use pandas to execute the query and return a DataFrame
        df = pd.read_sql(query, conn, params=params)
        
        logger.info(f"Successfully fetched {len(df)} rows as DataFrame")
        return df
    except Exception as e:
        logger.error(f"Error fetching DataFrame: {str(e)}")
        raise

def execute_batch(
    conn: 'teradatasql.TeradataConnection',
    query: str,
    params_list: List[Union[Tuple, Dict[str, Any], List]],
    commit: bool = True
) -> int:
    """
    Execute a batch query on a Teradata database.
    
    Args:
        conn: teradatasql connection
        query: SQL query
        params_list: List of query parameters
        commit: Whether to commit the transaction (default: True)
        
    Returns:
        Number of rows affected
        
    Raises:
        Exception: If the query cannot be executed
    """
    try:
        cursor = conn.cursor()
        cursor.executemany(query, params_list)
        
        if commit:
            conn.commit()
            
        row_count = cursor.rowcount
        cursor.close()
        
        logger.info(f"Successfully executed batch query, {row_count} rows affected")
        return row_count
    except Exception as e:
        logger.error(f"Error executing batch query: {str(e)}")
        if commit:
            conn.rollback()
        raise

def execute_fastload(
    conn: 'teradatasql.TeradataConnection',
    table_name: str,
    data: Union[List[Tuple], pd.DataFrame],
    columns: Optional[List[str]] = None,
    batch_size: int = 10000,
    commit: bool = True
) -> int:
    """
    Fast load data into a Teradata table.
    
    Args:
        conn: teradatasql connection
        table_name: Teradata table name
        data: Data to load (list of tuples or pandas DataFrame)
        columns: Column names (optional, required if data is a list of tuples)
        batch_size: Number of rows to insert in each batch (default: 10000)
        commit: Whether to commit the transaction (default: True)
        
    Returns:
        Number of rows loaded
        
    Raises:
        ValueError: If data is a list of tuples and columns is not provided
        Exception: If the data cannot be loaded
    """
    try:
        # Convert DataFrame to list of tuples if needed
        if isinstance(data, pd.DataFrame):
            if columns is None:
                columns = data.columns.tolist()
            data = [tuple(row) for row in data.values]
        elif columns is None:
            raise ValueError("columns must be provided if data is a list of tuples")
        
        # Build the INSERT statement
        placeholders = ', '.join(['?'] * len(columns))
        column_names = ', '.join(columns)
        insert_stmt = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"
        
        # Execute in batches
        cursor = conn.cursor()
        total_rows = 0
        
        for i in range(0, len(data), batch_size):
            batch = data[i:i+batch_size]
            cursor.executemany(insert_stmt, batch)
            total_rows += len(batch)
            logger.info(f"Loaded batch {i//batch_size + 1}, {len(batch)} rows")
        
        if commit:
            conn.commit()
            
        cursor.close()
        
        logger.info(f"Successfully loaded {total_rows} rows into {table_name}")
        return total_rows
    except Exception as e:
        logger.error(f"Error loading data: {str(e)}")
        if commit:
            conn.rollback()
        raise

def execute_fastexport(
    conn: 'teradatasql.TeradataConnection',
    query: str,
    params: Optional[Union[Tuple, Dict[str, Any], List]] = None,
    batch_size: int = 10000
) -> pd.DataFrame:
    """
    Fast export data from a Teradata query to a pandas DataFrame.
    
    Args:
        conn: teradatasql connection
        query: SQL query
        params: Query parameters (optional)
        batch_size: Number of rows to fetch in each batch (default: 10000)
        
    Returns:
        pandas DataFrame
        
    Raises:
        Exception: If the data cannot be exported
    """
    try:
        cursor = conn.cursor()
        cursor.execute(query, params)
        
        # Get column names
        column_names = [desc[0] for desc in cursor.description]
        
        # Fetch data in batches
        all_data = []
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            all_data.extend(rows)
            logger.info(f"Fetched {len(all_data)} rows so far")
        
        cursor.close()
        
        # Create DataFrame
        df = pd.DataFrame(all_data, columns=column_names)
        
        logger.info(f"Successfully exported {len(df)} rows to DataFrame")
        return df
    except Exception as e:
        logger.error(f"Error exporting data: {str(e)}")
        raise
