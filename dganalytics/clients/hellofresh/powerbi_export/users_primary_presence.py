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
                        sequence(date_sub(current_date(), 55), current_date() + 2,
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
                'users_primary_presence' pTableFlag,
                ut.region,
                startDate startDatePart
            FROM 
                gpc_hellofresh.fact_primary_presence fp, 
                user_timezone ut
            WHERE fp.userId = ut.userId
                AND startDate >= date_sub(current_date(), 35)
    """)
    pp.createOrReplaceTempView("users_primary_presence")

    df = spark.sql("""
                SELECT /*+ BROADCAST(dates) */  pp.UserKey, pp.userId, 
                    (case when cast(pp.startTime as date) < d.date then cast(d.date as timestamp) else pp.startTime end) startTime,
                    (case when cast(pp.endTime as date) > d.date then (cast((d.date + 1) as timestamp) - interval 1 second) else pp.endTime end) endTime,
                    systemPresence, 'users_primary_presence' pTableFlag, region, startDatePart
                 from
                    users_primary_presence pp, dates d 
                where d.date between cast(pp.startTime as date) and cast(pp.endTime as date)
            """)

    # Delete old data from the table for the last 5 days before inserting new data
    spark.sql(f"""
        DELETE FROM pbi_hellofresh.users_primary_presence 
        WHERE startDatePart >= date_sub(current_date(), 35)
    """)

    # Write new data to the target table
    df.write.mode("append").saveAsTable("pbi_hellofresh.users_primary_presence")

    return spark.sql(f"""SELECT * FROM pbi_hellofresh.users_primary_presence
                     WHERE startDatePart > add_months(current_date(), -12)
                     {"AND region IN ('US', 'CA-HF', 'CP-CA', 'CA-CP','FA-HF')" if region == 'US' else " " }""")
