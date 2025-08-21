"""
README for Job_EDW_ST_CNSMPTN_CMN_DIM_US_STG_TO_SUBS_DTL

This Python implementation replicates the functionality of the Talend ETL job Job_EDW_ST_CNSMPTN_CMN_DIM_US_STG_TO_SUBS_DTL.
"""

# Job_EDW_ST_CNSMPTN_CMN_DIM_US_STG_TO_SUBS_DTL

## Overview

This job processes subscription data from staging tables to create detailed subscription information for consumption data. The job extracts data from source tables, performs transformations including lookups to dimension tables, and loads the results into target staging tables. It handles both audio and streaming subscription data, enriching it with additional attributes from dimension tables.

## Architecture

The job follows a three-phase execution model:

1. **PreJob Phase**: 
   - Initializes context variables
   - Establishes database connections to Redshift
   - Records job start in audit tables

2. **Main Processing Phase**:
   - Determines processing path (CONSUMPTION or TRIP)
   - Truncates target tables
   - Executes SQL to transform and load data
   - Counts records for audit purposes

3. **PostJob Phase**:
   - Updates audit tables with processing statistics
   - Closes database connections
   - Records job completion

## Directory Structure

```
Job_EDW_ST_CNSMPTN_CMN_DIM_US_STG_TO_SUBS_DTL_MED_P1/
├── config.py                 # Configuration module
├── context.py                # Context variables
├── main.py                   # Main entry point
├── jobs/                     # Job modules
│   ├── Job_EDW_ST_CNSMPTN_CMN_DIM_US_STG_TO_SUBS_DTL/
│   │   ├── __init__.py
│   │   ├── main-job.py       # Main job implementation
│   │   ├── pre-job.py        # Pre-job implementation
│   │   └── post-job.py       # Post-job implementation
│   ├── Job_Frmwrk_EDW_BATCH_DETAIL_START/
│   │   ├── __init__.py
│   │   └── main-job.py       # Batch detail start job
│   └── Job_Frmwrk_EDW_BATCH_DETAIL_CLOSE/
│       ├── __init__.py
│       └── main-job.py       # Batch detail close job
├── joblets/                  # Joblet modules
│   ├── __init__.py
│   ├── jl_Frmwrk_EDW_LOAD_CONTEXT.py
│   └── Jl_Frmwrk_EDW_EXEC_LOG.py
└── utils/                    # Utility modules
    ├── __init__.py
    ├── context_utils.py      # Context utilities
    ├── database.py           # Database utilities
    ├── date_utils.py         # Date utilities
    ├── logging_utils.py      # Logging utilities
    └── parameter_store.py    # Parameter store utilities
```

## Usage

```bash
python main.py --env dev --flow-path CONSUMPTION --idl N
```

### Command Line Arguments

- `--env`: Environment (dev, test, prod)
- `--flow-path`: Processing path (CONSUMPTION or TRIP)
- `--idl`: IDL processing indicator (Y or N)
- `--start-exec-ts`: Start execution timestamp (required if idl=Y)
- `--end-exec-ts`: End execution timestamp (required if idl=Y)
- `--context-file`: Path to context file

## Dependencies

- Python 3.7+
- boto3
- psycopg2
- pandas

## Configuration

The job uses AWS Parameter Store for configuration. The following parameters are required:

- `/edo/{env}/redshift/host`
- `/edo/{env}/redshift/port`
- `/edo/{env}/redshift/database`
- `/edo/{env}/redshift/datamart/user`
- `/edo/{env}/redshift/datamart/password`
- `/edo/{env}/redshift/ods/user`
- `/edo/{env}/redshift/ods/password`
- `/edo/{env}/aws/kms_key`

## Context Variables

The job uses a large number of context variables to control its behavior. These are defined in `context.py` and can be overridden by command line arguments or a context file.