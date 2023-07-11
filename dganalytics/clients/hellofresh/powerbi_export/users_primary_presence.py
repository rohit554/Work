from dganalytics.utils.utils import get_spark_session, export_powerbi_csv, get_path_vars
from pyspark.sql import SparkSession
from dganalytics.connectors.gpc.gpc_utils import pb_export_parser, get_dbname, gpc_utils_logger
import os
import pandas as pd


def export_users_primary_presence(spark: SparkSession, tenant: str, region: str):

    tenant_path, db_path, log_path = get_path_vars(tenant)
    user_timezone = pd.read_csv(os.path.join(
        tenant_path, 'data', 'config', 'User_Group_region_Sites.csv'), header=0)
    user_timezone = spark.createDataFrame(user_timezone)
    user_timezone.createOrReplaceTempView("user_timezone")

    dates = spark.sql("""
                    select explode(date)  as date from (SELECT
                        sequence(add_months(current_date(), -13), current_date() + 2,
                                interval 1 day) as date)
                        """)
    dates.createOrReplaceTempView("dates")

    pp = spark.sql(f"""
            SELECT  /*+ BROADCAST(user_timezone) */ 
                fp.userId AS UserKey, 
                fp.userId, 
                from_utc_timestamp(fp.startTime, trim(ut.timeZone)) startTime,
                from_utc_timestamp(fp.endTime, trim(ut.timeZone)) endTime,
                fp.systemPresence,
                'users_primary_presence' pTableFlag
            FROM 
                fact_primary_presence fp, 
                user_timezone ut
            WHERE fp.userId = ut.userId
                AND ut.region {" = 'US'" if region == 'US' else " <> 'US'"}
                AND CAST(from_utc_timestamp(fp.startTime, trim(ut.timeZone)) AS date) >= date_sub(current_date, 365)
    """)
    pp.createOrReplaceTempView("primary_presence")

    df = spark.sql("""
                SELECT /*+ BROADCAST(dates) */  pp.UserKey, pp.userId, 
                    (case when cast(pp.startTime as date) < d.date then cast(d.date as timestamp) else pp.startTime end) startTime,
                    (case when cast(pp.endTime as date) > d.date then (cast((d.date + 1) as timestamp) - interval 1 second) else pp.endTime end) endTime,
                    systemPresence, 'users_primary_presence' pTableFlag
                 from
                    primary_presence pp, dates d 
                where d.date between cast(pp.startTime as date) and cast(pp.endTime as date)
            """)

    return df
