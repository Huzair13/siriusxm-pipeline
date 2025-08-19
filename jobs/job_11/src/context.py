# """
# Context variables for the Job_EDW_ST_CNSMPTN_CMN_DIM_US_STG_TO_SUBS_DTL job.

# This module defines all context variables used in the job, mirroring the Talend context variables.
# """
# import os
# import json
# import logging
# import datetime
# from typing import Dict, Any, Optional, Union

# from src.utils import s3_utils

# logger = logging.getLogger(__name__)

# class Context:
#     """
#     Context manager for the Job_EDW_ST_CNSMPTN_CMN_DIM_US_STG_TO_SUBS_DTL job.
    
#     This class provides a centralized way to store and access variables
#     that need to be shared across different parts of the job.
#     """
    
#     def __init__(self):
#         """
#         Initialize context variables with default values.
#         """
#         # Batch Audit Variables
#         self.batch_audit_detail_ERR_REC_QTY = 0
#         self.batch_audit_detail_FILE_NM = "N/A"
#         self.batch_audit_detail_FILE_RCVD_TS = None
#         self.batch_audit_detail_FILE_SIZE_IN_BYTES_QTY = 0
#         self.batch_audit_detail_id = 0
#         self.batch_audit_detail_INS_REC_QTY = 0
#         self.batch_audit_detail_SRC_REC_QTY = 0
#         self.batch_audit_detail_UPD_REC_QTY = 0
#         self.batch_id = 0
#         self.batch_detail_id = 0
#         self.INS_REC_QTY = 0
#         self.SRC_REC_QTY = 0
        
#         # Batch Configuration Variables
#         self.batch_compare_date = False
#         self.batch_env = "dev"
#         self.batch_folder_prefix = ""
#         self.batch_job_name = "Job_EDW_ST_CNSMPTN_CMN_DIM_US_STG_TO_SUBS_DTL"
#         self.batch_manifestjson = ""
#         self.batch_process_date = datetime.datetime.now()
#         self.batch_src_s3_file_delimiter = ","
#         self.batch_src_s3_file_type = ""
#         self.batch_src_tbl_name = ""
#         self.batch_status = ""
#         self.batch_subject_area_id = "1"  # Default value, should be set from parameters
#         self.batch_tgt_tbl_name = ""
        
#         # Process Control Variables
#         self.pc_cdc_field_nm = ""
#         self.pc_end_exec_offset_hr_qty = 0
#         self.pc_end_exec_ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#         self.pc_interval_nm = ""
#         self.pc_interval_qty = 0
#         self.pc_is_idl_ind = "N"  # Default to non-IDL processing
#         self.pc_src_pltfrm_nm = ""
#         self.pc_src_timezone_cd = ""
#         self.pc_start_exec_offset_hr_qty = 0
#         self.pc_start_exec_ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
#         # File Configuration Variables
#         self.file_log_file_nm = "jobname_log.txt"
#         self.file_manifest_file_nm = ""
#         self.file_inprogress_file_nm = ""
#         self.file_complete_file_nm = ""
#         self.file_status_file_nm = "jobname_status.txt"
#         self.file_unique_filelist_nm = ""
        
#         # Integration Configuration Variables
#         self.ic_business_email_addr_txt = ""
#         self.ic_dept_nm = ""
#         self.ic_division_nm = ""
#         self.ic_etl_local_home_path_nm = "/tmp/"
#         self.ic_etl_log_path_nm = ""
#         self.ic_etl_manifest_file_path_nm = ""
#         self.ic_etl_param_file_path_nm = ""
#         self.ic_etl_s3_bucket_nm = ""
#         self.ic_etl_s3_home_path_nm = ""
#         self.ic_is_multiple_source_ind = ""
#         self.ic_subject_area_nm = ""
#         self.ic_tech_email_addr_txt = ""
        
#         # Parameter Variables
#         self.parameter_file_context_file_nm = "context.properties"
#         self.parameter_file_local_context_path = "/tmp/"
#         self.parameter_workflow_name = "Job_EDW_ST_CNSMPTN_CMN_DIM_US_STG_TO_SUBS_DTL"
#         self.parameter_dept_nm = ""
#         self.parameter_division_nm = ""
#         self.parameter_env = "dev"
#         self.parameter_subject_area_nm = ""
        
#         # Job-Specific Variables
#         self.from_dt = ""
#         self.to_dt = ""
#         self.db_edw_ods_schema = "edw_ods"
#         self.db_edw_datamart_schema = "edw_datamart_stg"
#         self.flow_path = "CONSUMPTION"  # Default to CONSUMPTION path
#         self.stg_src_table = "edw_datamart_stg.stg_si_consumption"
#         self.stg_table_dt_subs = "edw_datamart_stg.stg_st_consumption_dt_subs"
#         self.stg_table_dt_subs_dtl = "edw_datamart_stg.stg_st_consumption_dt_subs_dtl"
#         self.ods_table_subscription_link = "consumption_subscription"
#         self.start_est_dt = "brdcst_start_est_dt"
#         self.link_id = "cnsmptn_id"
#         self.dt_subs_date_column = "brdcst_start_est_dt"
#         self.lookup_buffer_days = 7
#         self.cutoff_process_nm = "Fact_Summary"
#         self.father_pid = "0"
#         self.message_type = "info"
#         self.pid = "0"
    
#     def set(self, name: str, value: Any) -> None:
#         """
#         Set a context variable.
        
#         Args:
#             name: Variable name
#             value: Variable value
#         """
#         if hasattr(self, name):
#             setattr(self, name, value)
#             logger.debug(f"Context variable '{name}' set to '{value}'")
#         else:
#             logger.warning(f"Attempted to set unknown context variable: {name}")
    
#     def get(self, name: str, default: Any = None) -> Any:
#         """
#         Get a context variable.
        
#         Args:
#             name: Variable name
#             default: Default value if the variable is not found
            
#         Returns:
#             Variable value or default if not found
#         """
#         return getattr(self, name, default)
    
#     def update(self, variables: Dict[str, Any]) -> None:
#         """
#         Update multiple context variables.
        
#         Args:
#             variables: Dictionary of variable names and values
#         """
#         for name, value in variables.items():
#             self.set(name, value)
    
#     def load_from_env(self, prefix: str = "", include: Optional[list] = None) -> None:
#         """
#         Load context variables from environment variables.
        
#         Args:
#             prefix: Prefix for environment variables to load (e.g., "GLUE_")
#             include: List of specific environment variables to include (without prefix)
#         """
#         for key, value in os.environ.items():
#             if prefix and not key.startswith(prefix):
#                 continue
                
#             if include and key[len(prefix):] not in include:
#                 continue
                
#             # Remove prefix from key
#             context_key = key[len(prefix):]
            
#             # Try to parse as JSON for complex types
#             try:
#                 parsed_value = json.loads(value)
#                 self.set(context_key, parsed_value)
#             except (json.JSONDecodeError, TypeError):
#                 # If not valid JSON, use as string
#                 self.set(context_key, value)
    
#     def load_from_job_params(self, params: Dict[str, Any]) -> None:
#         """
#         Load context variables from job parameters.
        
#         Args:
#             params: Dictionary of job parameters
#         """
#         for key, value in params.items():
#             self.set(key, value)
    
#     def load_from_file(self, file_path: str) -> None:
#         """
#         Load context variables from a JSON file.
        
#         Args:
#             file_path: Path to the JSON file
#         """
#         try:
#             with open(file_path, 'r') as f:
#                 variables = json.load(f)
                
#             self.update(variables)
#             logger.info(f"Loaded {len(variables)} context variables from {file_path}")
#         except Exception as e:
#             logger.error(f"Failed to load context from {file_path}: {str(e)}")
    
#     def load_from_s3(self, bucket: str, key: str, region_name: Optional[str] = None) -> None:
#         """
#         Load context variables from a JSON file in S3.
        
#         Args:
#             bucket: S3 bucket name
#             key: S3 object key
#             region_name: AWS region name (optional)
#         """
#         try:
#             # Read the JSON file from S3
#             content = s3_utils.read_s3_file_to_string(bucket, key, region_name=region_name)
            
#             # Parse the JSON content
#             variables = json.loads(content)
            
#             # Add the variables to the context
#             self.update(variables)
            
#             logger.info(f"Loaded {len(variables)} context variables from S3 bucket '{bucket}', key '{key}'")
#         except Exception as e:
#             logger.error(f"Failed to load context from S3 bucket '{bucket}', key '{key}': {str(e)}")
#             raise

# # Create a global context instance
# context = Context()


"""
Context variables for the Job_EDW_ST_CNSMPTN_CMN_DIM_US_STG_TO_SUBS_DTL job.

This module defines all context variables used in the job, mirroring the Talend context variables.
"""
import os
import json
import logging
import datetime
from typing import Dict, Any, Optional, Union

from src.utils import s3_utils

logger = logging.getLogger(__name__)


def load_properties_file(file_path: str) -> Dict[str, str]:
    """
    Load a Java-style .properties file and return a dict.
    """
    props = {}
    try:
        with open(file_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    if '=' in line:
                        k, v = line.split('=', 1)
                        props[k.strip()] = v.strip()
        logger.info(f"Loaded {len(props)} variables from properties file {file_path}")
    except Exception as e:
        logger.error(f"Error reading properties file {file_path}: {str(e)}")
    return props


class Context:
    """
    Context manager for the Job_EDW_ST_CNSMPTN_CMN_DIM_US_STG_TO_SUBS_DTL job.

    This class provides a centralized way to store and access variables
    that need to be shared across different parts of the job.
    """

    def __init__(self):
        # ... (unchanged init as you provided, omitted for brevity)
        # Paste your original __init__ here
        # See note below

        # Example for brevity (keep your full list):
        self.batch_audit_detail_ERR_REC_QTY = 0
        self.batch_audit_detail_FILE_NM = "N/A"
        self.batch_audit_detail_FILE_RCVD_TS = None
        self.batch_audit_detail_FILE_SIZE_IN_BYTES_QTY = 0
        # ...etc...

    def set(self, name: str, value: Any) -> None:
        if hasattr(self, name):
            setattr(self, name, value)
            logger.debug(f"Context variable '{name}' set to '{value}'")
        else:
            logger.warning(f"Attempted to set unknown context variable: {name}")

    def get(self, name: str, default: Any = None) -> Any:
        return getattr(self, name, default)

    def update(self, variables: Dict[str, Any]) -> None:
        for name, value in variables.items():
            self.set(name, value)

    def remove(self, name: str) -> None:
        """
        Remove a context variable if it exists.
        """
        if hasattr(self, name):
            delattr(self, name)
            logger.info(f"Context variable '{name}' removed")
        else:
            logger.warning(f"Tried to remove unknown variable '{name}'")

    def load_from_env(self, prefix: str = "", include: Optional[list] = None) -> None:
        for key, value in os.environ.items():
            if prefix and not key.startswith(prefix):
                continue
            if include and key[len(prefix):] not in include:
                continue
            context_key = key[len(prefix):]
            try:
                parsed_value = json.loads(value)
                self.set(context_key, parsed_value)
            except (json.JSONDecodeError, TypeError):
                self.set(context_key, value)

    def load_from_job_params(self, params: Dict[str, Any]) -> None:
        for key, value in params.items():
            self.set(key, value)

    def load_from_file(self, file_path: str) -> None:
        """
        Load context variables from a file.
        Supports both JSON (.json) and Java .properties files (.properties).
        """
        if not os.path.exists(file_path):
            logger.warning(f"Context file {file_path} not found, skipping load.")
            return

        try:
            if file_path.endswith(".json"):
                with open(file_path, 'r') as f:
                    variables = json.load(f)
                self.update(variables)
                logger.info(f"Loaded {len(variables)} context variables from JSON file {file_path}")

            elif file_path.endswith(".properties"):
                variables = load_properties_file(file_path)
                self.update(variables)
                logger.info(f"Loaded {len(variables)} context variables from properties file {file_path}")

            else:
                logger.error(f"Unsupported context file format: {file_path}")
        except Exception as e:
            logger.error(f"Failed to load context from {file_path}: {str(e)}")

    def load_from_s3(self, bucket: str, key: str, region_name: Optional[str] = None) -> None:
        """
        Load context variables from a JSON or properties file in S3.
        """
        try:
            content = s3_utils.read_s3_file_to_string(bucket, key, region_name=region_name)
            if key.endswith(".json"):
                variables = json.loads(content)
            elif key.endswith(".properties"):
                # Parse properties from string
                variables = {}
                for line in content.splitlines():
                    line = line.strip()
                    if line and not line.startswith('#'):
                        if '=' in line:
                            k, v = line.split('=', 1)
                            variables[k.strip()] = v.strip()
            else:
                logger.error(f"Unsupported context file format in S3: {key}")
                return

            self.update(variables)
            logger.info(f"Loaded {len(variables)} context variables from S3 bucket '{bucket}', key '{key}'")
        except Exception as e:
            logger.error(f"Failed to load context from S3 bucket '{bucket}', key '{key}': {str(e)}")
            raise

# Create a global context instance
context = Context()

