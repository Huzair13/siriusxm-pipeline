"""
Main job tasks for the Job_EDW_ST_CNSMPTN_CMN_DIM_US_STG_TO_SUBS_DTL job.

This module contains functions for the main processing phase of the Job_EDW_ST_CNSMPTN_CMN_DIM_US_STG_TO_SUBS_DTL job.
"""
import logging
import datetime
from typing import Dict, Any, Optional, Tuple

from src.context import context
from src.utils import redshift_utils, logging_utils

logger = logging.getLogger(__name__)

def set_date_range_idl(start_exec_ts: str, end_exec_ts: str) -> Tuple[str, str]:
    """
    Set date range for IDL processing.
    
    Args:
        start_exec_ts: Start execution timestamp
        end_exec_ts: End execution timestamp
        
    Returns:
        Tuple of (from_dt, to_dt)
    """
    try:
        # Parse timestamps and format as dates
        from_dt = datetime.datetime.strptime(start_exec_ts, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d")
        to_dt = datetime.datetime.strptime(end_exec_ts, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d")
        
        logger.info(f"Set date range for IDL processing: {from_dt} to {to_dt}")
        return from_dt, to_dt
    except Exception as e:
        logger.error(f"Failed to set date range for IDL processing: {str(e)}")
        raise

def get_cutoff_dates(conn) -> Tuple[str, str]:
    """
    Get cutoff dates for non-IDL processing.
    
    Args:
        conn: Redshift connection
        
    Returns:
        Tuple of (from_dt, to_dt)
        
    Raises:
        Exception: If cutoff dates cannot be retrieved
    """
    try:
        query = f"""
        SELECT 
        start_exec_ts, start_exec_dt, cnsmptn_cutoff_dt,
        CASE WHEN start_exec_dt<=dateadd(day,1,cnsmptn_cutoff_dt) 
         THEN first_day_of_prev_month ELSE first_day_of_curr_month END as from_dt,
        start_exec_dt as to_dt
        FROM(
        SELECT PC.*,
        MAX(COFF.cnsmptn_cutoff_dt) cnsmptn_cutoff_dt
        FROM
        (
        select start_exec_ts,
        start_exec_ts::DATE as start_exec_dt,
          ADD_MONTHS(start_exec_ts, -1) PREV_MONTH_DT,
        date_part('year',ADD_MONTHS(start_exec_ts, -1))::integer as PREV_MONTH_YEAR,
        date_part('month',ADD_MONTHS(start_exec_ts, -1))::integer as PREV_MONTH,
        DATE_TRUNC('month', PREV_MONTH_DT)::date as  first_day_of_prev_month,
        DATE_TRUNC('month', start_exec_ts)::date as first_day_of_curr_month, 
        date_part('month',start_exec_ts)::integer as CURR_MONTH
        FROM edw_ods.process_control where process_nm='{context.cutoff_process_nm}') PC
        JOIN edw_ods.consumption_cutoff COFF
        ON PC.PREV_MONTH_YEAR=COFF.brdcst_year_nbr
        AND PC.PREV_MONTH=COFF.brdcst_month_nbr
        GROUP BY 1,2,3,4,5,6,7,8
        );
        """
        
        result = redshift_utils.fetch_one(conn, query)
        
        if result and len(result) >= 5:
            from_dt = result[3]
            to_dt = result[4]
            
            # Convert to string format
            from_dt = from_dt.strftime("%Y-%m-%d") if isinstance(from_dt, datetime.date) else str(from_dt)
            to_dt = to_dt.strftime("%Y-%m-%d") if isinstance(to_dt, datetime.date) else str(to_dt)
            
            logger.info(f"Retrieved cutoff dates: {from_dt} to {to_dt}")
            return from_dt, to_dt
        else:
            logger.error("No cutoff dates found")
            raise ValueError("No cutoff dates found")
    except Exception as e:
        logger.error(f"Failed to get cutoff dates: {str(e)}")
        raise

def process_consumption_path(datamart_conn, from_dt: str, to_dt: str, lookup_buffer_days: int) -> None:
    """
    Process the CONSUMPTION path.
    
    Args:
        datamart_conn: Redshift datamart connection
        from_dt: Start date
        to_dt: End date
        lookup_buffer_days: Number of buffer days for lookup
        
    Raises:
        Exception: If processing fails
    """
    try:
        # SQL query for CONSUMPTION path
        query = f"""
        truncate table {context.stg_table_dt_subs};

        Insert into {context.stg_table_dt_subs}(
        {context.dt_subs_date_column},
        sbscrptn_key_id,
        cnsmd_srvc_cd,
        cnsumd_veh_device_id,
        sbscrptn_subtype_prdct_catg_cd
        )
        select {context.dt_subs_date_column},
        sbscrptn_key_id,
        cnsmd_srvc_cd,
        cnsumd_veh_device_id,
        sbscrptn_subtype_prdct_catg_cd 
        from(
        select 
        tmp.{context.dt_subs_date_column},
        tmp.sbscrptn_key_id,
        tmp.cnsmd_srvc_cd,
        tmp.cnsumd_veh_device_id,
        tmp.sbscrptn_subtype_prdct_catg_cd,
        row_number() over(partition by tmp.{context.dt_subs_date_column},tmp.sbscrptn_key_id,tmp.cnsmd_srvc_cd order by tmp.rec_cnt desc) rnk
        from(
        SELECT distinct 
        a.{context.dt_subs_date_column},
        b.sbscrptn_key_id,
        a.cnsmd_srvc_cd,
        a.device_id as cnsumd_veh_device_id,
        b.sbscrptn_subtype_prdct_catg_cd,
        count(1) rec_cnt
        FROM {context.stg_src_table} a
        join edw_ods.{context.ods_table_subscription_link} b on a.{context.link_id}=b.{context.link_id}
        where a.{context.start_est_dt} >='{from_dt}'-{lookup_buffer_days} and a.{context.start_est_dt} <'{to_dt}'+{lookup_buffer_days}
        group by 1,2,3,4,5
        ) tmp
        )tmp1 where rnk=1
        """
        
        # Execute the query
        redshift_utils.execute_query(datamart_conn, query)
        
        logger.info("Successfully processed CONSUMPTION path")
    except Exception as e:
        logger.error(f"Failed to process CONSUMPTION path: {str(e)}")
        raise

def process_trip_path(datamart_conn, from_dt: str, to_dt: str, lookup_buffer_days: int) -> None:
    """
    Process the TRIP path.
    
    Args:
        datamart_conn: Redshift datamart connection
        from_dt: Start date
        to_dt: End date
        lookup_buffer_days: Number of buffer days for lookup
        
    Raises:
        Exception: If processing fails
    """
    try:
        # SQL query for TRIP path
        query = f"""
        drop table if exists tmp_st_device_trip_dt_subs;

        create temp table tmp_st_device_trip_dt_subs as
        SELECT distinct 
        a.trip_start_est_ts::date as trip_start_est_dt,
        a.trip_end_est_ts::date as trip_end_est_dt,
        b.sbscrptn_key_id,
        'Audio' as cnsmd_srvc_cd,
        a.device_id::varchar(20) as cnsumd_veh_device_id,
        b.sbscrptn_subtype_prdct_catg_cd,
        count(1) rec_cnt
        FROM {context.stg_src_table} a
        join edw_ods.{context.ods_table_subscription_link} b on a.{context.link_id}=b.{context.link_id}
        where a.trip_start_est_ts::date>='{from_dt}'-{lookup_buffer_days} and a.trip_start_est_ts::date <'{to_dt}'+{lookup_buffer_days}
        group by 1,2,3,4,5,6;

        Truncate table {context.stg_table_dt_subs};

        Insert into {context.stg_table_dt_subs}(
        trip_start_est_dt,
        sbscrptn_key_id,
        cnsmd_srvc_cd,
        cnsumd_veh_device_id,
        sbscrptn_subtype_prdct_catg_cd
        )
        select trip_start_est_dt,
        sbscrptn_key_id,
        cnsmd_srvc_cd,
        cnsumd_veh_device_id,
        sbscrptn_subtype_prdct_catg_cd  
        from(
        select 
        tmp.trip_start_est_dt,
        tmp.sbscrptn_key_id,
        tmp.cnsmd_srvc_cd,
        tmp.cnsumd_veh_device_id,
        tmp.sbscrptn_subtype_prdct_catg_cd,
        row_number() over(partition by tmp.trip_start_est_dt,tmp.sbscrptn_key_id,tmp.cnsmd_srvc_cd order by tmp.rec_cnt desc) rnk
        from(
        SELECT distinct
        dd.clndr_dt as trip_start_est_dt,
        a.sbscrptn_key_id,
        a.cnsmd_srvc_cd,
        a.cnsumd_veh_device_id,
        a.sbscrptn_subtype_prdct_catg_cd,
        rec_cnt
        FROM tmp_st_device_trip_dt_subs a
        join edw_datamart.dim_date dd on dd.clndr_dt between a.trip_start_est_dt and a.trip_end_est_dt
        ) tmp
        )tmp1 where rnk=1;
        """
        
        # Execute the query
        redshift_utils.execute_query(datamart_conn, query)
        
        logger.info("Successfully processed TRIP path")
    except Exception as e:
        logger.error(f"Failed to process TRIP path: {str(e)}")
        raise

def process_subscription_detail(datamart_conn) -> None:
    """
    Process subscription detail data.
    
    Args:
        datamart_conn: Redshift datamart connection
        
    Raises:
        Exception: If processing fails
    """
    try:
        # SQL query for subscription detail
        query = f"""
        truncate table {context.stg_table_dt_subs_dtl};

        Insert into {context.stg_table_dt_subs_dtl}(
        {context.dt_subs_date_column},
        sbscrptn_key_id,
        cnsmd_srvc_cd,
        sbscrptn_id,
        short_sbscrptn_id,
        sbscrbr_id,
        audio_srvc_id,
        strmng_srvc_id,
        srvc_subtype_cd,
        srvc_type_cd,
        trial_start_est_dt,
        trial_end_est_dt,
        trial_actvtn_est_dt,
        trial_durn_cd,
        sbscrptn_start_dt,
        sbscrptn_end_dt,
        strmng_promo_key_id,
        strmng_promo_cd,
        strmng_rgstrtn_zip_cd,
        cnsumd_veh_device_id,
        sbscrptn_device_id,
        create_ts,
        sbscrptn_subtype_prdct_catg_cd
        )
        select
        a.{context.dt_subs_date_column},
        a.sbscrptn_key_id,
        a.cnsmd_srvc_cd,
        b.sbscrptn_id,
        b.short_sbscrptn_id,
        b.sbscrbr_id,
        b.audio_srvc_id,
        b.strmng_srvc_id,
        case when a.cnsmd_srvc_cd='Audio' then b.curr_audio_srvc_subtype_cd else b.curr_strmng_srvc_subtype_cd end as srvc_subtype_cd,
        coalesce(case when a.cnsmd_srvc_cd='Audio' then AUDSRVC.srvc_type_cd else STRSRVC.srvc_type_cd end,'N/D') as srvc_type_cd,
        Case When AUDSRVC.srvc_type_cd = 'T' Then COALESCE(b.AUDIO_TRIAL_START_DT, '2999-12-31')             
             When (STRSRVC.srvc_type_cd = 'T' OR b.sbscrptn_subtype_prdct_catg_cd='AFL') Then COALESCE(b.STRMNG_TRIAL_START_DT, '2999-12-31')     
             Else '1970-01-01'    End As     trial_start_est_dt,    
        Case When AUDSRVC.srvc_type_cd = 'T' Then COALESCE(b.AUDIO_TRIAL_END_DT, '2999-12-31')            
             When (STRSRVC.srvc_type_cd = 'T' OR b.sbscrptn_subtype_prdct_catg_cd='AFL') Then COALESCE(b.STRMNG_TRIAL_END_DT, '2999-12-31')        
             Else '1970-01-01'    End As     trial_end_est_dt,        
        Case When a.cnsmd_srvc_cd = 'Audio' Then '2999-12-31'
        Else COALESCE(Trunc(b.STRMNG_ACTVTN_TS), '2999-12-31') End As trial_actvtn_est_dt,
        b.STRMNG_PROMO_DURN_DAY_MONTH_CD As trial_durn_cd,
        Case When AUDSRVC.srvc_type_cd = 'S' Then COALESCE(b.AUDIO_SELF_PAY_START_DT, '2999-12-31')            
        When STRSRVC.srvc_type_cd = 'S' Then COALESCE(b.STRMNG_SELF_PAY_START_DT, '2999-12-31')    
        Else '1970-01-01'    End As     sbscrptn_start_dt,        
        Case When AUDSRVC.srvc_type_cd = 'S' Then COALESCE(b.AUDIO_SELF_PAY_END_DT, '2999-12-31')            
             When STRSRVC.srvc_type_cd = 'S' Then COALESCE(b.strmng_self_pay_end_dt, '2999-12-31')
             Else '1970-01-01'    End As     sbscrptn_end_dt,
        c.strmng_promo_key_id,
        b.strmng_promo_cd,
        b.strmng_rgstrtn_zip_cd,
        UPPER(a.cnsumd_veh_device_id)  as cnsumd_veh_device_id,
        UPPER(b.device_id)as sbscrptn_device_id,
        sysdate as create_ts,
        a.sbscrptn_subtype_prdct_catg_cd
        from
        {context.stg_table_dt_subs} a
        left outer join edw_datamart.dim_subscription b on b.sbscrptn_key_id=a.sbscrptn_key_id
        left outer join edw_datamart.DIM_STREAMING_PROMO c on c.strmng_promo_cd=b.strmng_promo_cd and a.{context.dt_subs_date_column} between c.rec_eff_ts and c.rec_exp_ts
        left outer join edw_ods.v_service_subtype AUDSRVC ON (CASE WHEN a.cnsmd_srvc_cd = 'Audio' THEN b.curr_audio_srvc_subtype_cd ELSE 'N/A' END) = AUDSRVC.srvc_subtype_cd
        LEFT OUTER JOIN edw_ods.v_service_subtype STRSRVC ON (CASE WHEN a.cnsmd_srvc_cd = 'SIR' THEN b.curr_strmng_srvc_subtype_cd ELSE 'N/A' END) = STRSRVC.srvc_subtype_cd;
        """
        
        # Execute the query
        redshift_utils.execute_query(datamart_conn, query)
        
        logger.info("Successfully processed subscription detail data")
    except Exception as e:
        logger.error(f"Failed to process subscription detail data: {str(e)}")
        raise

def count_records(datamart_conn) -> Tuple[int, int]:
    """
    Count records in source and target tables.
    
    Args:
        datamart_conn: Redshift datamart connection
        
    Returns:
        Tuple of (source_count, target_count)
        
    Raises:
        Exception: If record counting fails
    """
    try:
        # Count source records
        source_query = f"select count(*) from {context.stg_table_dt_subs};"
        source_result = redshift_utils.fetch_one(datamart_conn, source_query)
        source_count = source_result[0] if source_result else 0
        
        # Count target records
        target_query = f"select count(*) from {context.stg_table_dt_subs_dtl};"
        target_result = redshift_utils.fetch_one(datamart_conn, target_query)
        target_count = target_result[0] if target_result else 0
        
        logger.info(f"Record counts - Source: {source_count}, Target: {target_count}")
        return source_count, target_count
    except Exception as e:
        logger.error(f"Failed to count records: {str(e)}")
        raise

def run_main_job(pre_job_results: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Run main job tasks for the Job_EDW_ST_CNSMPTN_CMN_DIM_US_STG_TO_SUBS_DTL job.
    
    Args:
        pre_job_results: Results from pre-job tasks
        config: Configuration dictionary
        
    Returns:
        Dictionary of job results
        
    Raises:
        Exception: If main job tasks fail
    """
    try:
        # Get database connections from pre-job results
        datamart_conn = pre_job_results.get("datamart_conn")
        ods_conn = pre_job_results.get("ods_conn")
        
        if not datamart_conn or not ods_conn:
            raise ValueError("Database connections not found in pre-job results")
        
        # Determine date range
        if context.pc_is_idl_ind == "Y":
            # IDL processing path
            from_dt, to_dt = set_date_range_idl(context.pc_start_exec_ts, context.pc_end_exec_ts)
        else:
            # Non-IDL processing path
            from_dt, to_dt = get_cutoff_dates(ods_conn)
        
        # Set date range in context
        context.from_dt = from_dt
        context.to_dt = to_dt
        
        # Process based on flow path
        if context.flow_path == "TRIP":
            # TRIP processing path
            process_trip_path(datamart_conn, from_dt, to_dt, context.lookup_buffer_days)
        else:
            # CONSUMPTION processing path (default)
            process_consumption_path(datamart_conn, from_dt, to_dt, context.lookup_buffer_days)
        
        # Process subscription detail
        process_subscription_detail(datamart_conn)
        
        # Count records
        source_count, target_count = count_records(datamart_conn)
        
        # Set record counts in context
        context.SRC_REC_QTY = source_count
        context.INS_REC_QTY = target_count
        
        # Calculate error records
        context.batch_audit_detail_SRC_REC_QTY = source_count
        context.batch_audit_detail_INS_REC_QTY = target_count
        context.batch_audit_detail_ERR_REC_QTY = source_count - target_count
        
        logger.info("Main job tasks completed successfully")
        
        return {
            "source_count": source_count,
            "target_count": target_count,
            "error_count": source_count - target_count
        }
    except Exception as e:
        logger.error(f"Main job tasks failed: {str(e)}")
        raise