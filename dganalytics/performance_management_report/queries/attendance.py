from dganalytics.utils.utils import exec_mongo_pipeline, get_path_vars, get_active_organization_timezones
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, to_timestamp, lower
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from datetime import datetime, timedelta
import pandas as pd
import os

def get_hf_timezones(spark):
    tenant_path, db_path, log_path = get_path_vars("hellofresh")
    user_timezone = pd.read_csv(os.path.join(tenant_path, 'data', 'config', 'User_Group_region_Sites.csv'), header=0)
    user_timezone = spark.createDataFrame(user_timezone)
    return user_timezone

def save_genesys_clients_attendance(spark, tenant, orgId):
    user_timezone = get_hf_timezones(spark)
    user_timezone.createOrReplaceTempView("user_timezone")
    extract_start_time = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%S.%fZ')[:-4] + 'Z'

    df = spark.sql(f"""
        SELECT 
            fp.userId, 
            TRUE AS isPresent,
            from_utc_timestamp(
                lag(fp.endTime, 1) OVER (PARTITION BY fp.userId ORDER BY fp.startTime),
                trim(ut.timeZone)
            ) AS tZStartTime,
            CASE 
                WHEN tZStartTime IS NULL 
                    AND CAST((unix_timestamp(fp.endTime) - unix_timestamp(fp.startTime)) / 3600 AS INT) > 12 
                THEN 
                    from_utc_timestamp(fp.startTime - INTERVAL 12 HOURS, trim(ut.timeZone)) 
                ELSE 
                    from_utc_timestamp(
                        lag(fp.endTime, 1) OVER (PARTITION BY fp.userId ORDER BY fp.startTime), 
                        trim(ut.timeZone)
                    )
            END AS loginTime,
            from_utc_timestamp(fp.startTime, trim(ut.timeZone)) AS logoutTime,
            '{datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')}' AS recordInsertDate,
            pmu.orgId,
            to_date(from_utc_timestamp(fp.startTime, trim(ut.timeZone)), 'dd-MM-yyyy') AS reportDate
        FROM  gpc_{tenant}.fact_primary_presence fp
        JOIN 
            (SELECT * FROM dg_performance_management.users WHERE orgId = '{orgId}') pmu
        ON 
            fp.userId = pmu.userId
        JOIN 
            user_timezone ut
        ON 
            fp.userId = ut.userId
        WHERE 
            fp.systemPresence = 'OFFLINE'
            AND CAST((unix_timestamp(fp.endTime) - unix_timestamp(fp.startTime)) / 3600 AS INT) > 4
            AND from_utc_timestamp(fp.startTime, trim(ut.timeZone)) >= TIMESTAMP('{extract_start_time}')
    """)
    
    df.createOrReplaceTempView("attendance")
    spark.sql(f"""
        MERGE INTO dg_performance_management.attendance target
        USING attendance source
        ON date_format(cast(source.reportDate AS date), 'dd-MM-yyyy') = date_format(cast(target.reportDate AS date), 'dd-MM-yyyy')
        AND source.userId = target.userId
        AND source.orgId = target.orgId
        AND source.loginTime = target.loginTime
        AND source.logoutTime = target.logoutTime
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
    """)

def get_attendance(spark):
    extract_start_time = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%S.%fZ')[:-4] + 'Z'
    # excluded_orgs = ['hellofreshanz', 'tpindiait']
    
    for org_timezone in get_active_organization_timezones(spark).rdd.collect():
        org_id = org_timezone['org_id'].lower()

        #For Genesys Orgs
        if org_id in ['hellofreshanz']:
            tenants = {'hellofreshanz': 'hellofresh'}
            df = save_genesys_clients_attendance(spark, tenants[org_id], org_id)
            continue
        # For Orgs sharing attendance data
        elif org_id in  ['tpindiait']:
            continue

        df = spark.sql(f"""
        SELECT 
            userId,
            report_date AS reportDate,
            CASE 
                WHEN COUNT(attr_value) > 0 THEN TRUE 
                ELSE FALSE 
            END AS isPresent,
            orgId,
            '{datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')}' AS recordInsertDate,
            CAST(report_date AS TIMESTAMP) AS loginTime,
            CAST(report_date AS TIMESTAMP) + INTERVAL 8 HOUR AS logoutTime
        FROM 
            dg_performance_management.kpi_data
        WHERE 
            orgId = '{org_id}' 
            AND report_date >= '{extract_start_time}'
        GROUP BY 
            userId, report_date, orgId
        """)
        
        df.createOrReplaceTempView("attendance")
        spark.sql(f"""
            MERGE INTO dg_performance_management.attendance target
            USING attendance source
            ON date_format(cast(source.reportDate AS date), 'dd-MM-yyyy') = date_format(cast(target.reportDate AS date), 'dd-MM-yyyy')
            AND source.userId = target.userId
            AND source.orgId = target.orgId
            AND source.loginTime = target.loginTime
            AND source.logoutTime = target.logoutTime
            WHEN MATCHED THEN
                UPDATE SET *
            WHEN NOT MATCHED THEN
                INSERT *
        """)