from dganalytics.utils.utils import get_spark_session, export_powerbi_csv, get_path_vars
from pyspark.sql import SparkSession
from dganalytics.connectors.gpc.gpc_utils import pb_export_parser, get_dbname, gpc_utils_logger
import os
import pandas as pd
import pytz
from datetime import datetime
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType

def is_valid_date(dt=None, timezone="UTC"):
    dt = datetime.fromisoformat(dt).replace(tzinfo=None)
    try:
        if dt is None:
            dt = datetime.utcnow()
        timezone = pytz.timezone(timezone)
        timezone_aware_date = timezone.localize(dt, is_dst=True)
        is_dst = timezone_aware_date.tzinfo._dst.seconds != 0
        if(is_dst):
            timezone_aware_date = timezone.localize(dt, is_dst=None)
        return True
    except Exception as e:
        print(e)
        return False

def export_users_routing_status_sliced(spark: SparkSession, tenant: str, region: str):
    tenant_path, db_path, log_path = get_path_vars(tenant)
    user_timezone = pd.read_csv(os.path.join(
        tenant_path, 'data', 'config', 'User_Group_region_Sites.csv'), header=0)
    user_timezone = spark.createDataFrame(user_timezone)
    user_timezone.createOrReplaceTempView("user_timezone")

    routing = spark.sql(f"""
            SELECT
            frs.userId AS userKey,
            from_utc_timestamp(frs.startTime, trim(ut.timeZone)) AS startTime,
            from_utc_timestamp(frs.endTime, trim(ut.timeZone)) AS endTime,
            frs.routingStatus AS routingStatus,
            from_utc_timestamp(frs.startTime, trim(ut.timeZone)) AS timeSlot,
            1800 AS timeDiff,
            'users_routing_status' AS pTableFlag,
            ut.timeZone AS timeZone
        FROM
            gpc_hellofresh.fact_routing_status frs
            JOIN user_timezone ut ON frs.userId = ut.userId
        WHERE
            frs.startDate >= cast('2020-02-01' AS date)
            AND ut.region {" = 'US'" if region == 'US' else " <> 'US'"}
            AND CAST(from_utc_timestamp(frs.startTime, trim(ut.timeZone)) AS date) >= date_sub(current_date, 365)
    """)

    routing.createOrReplaceTempView("routing")
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    spark.udf.register("is_valid_date_udf", is_valid_date, BooleanType())

    df = spark.sql("""
        SELECT
            userKey AS userKey,
            date_format((CASE WHEN startTime > from_unixtime(timeSlot) THEN startTime ELSE from_unixtime(timeSlot) END), 'yyyy-MM-dd HH:mm:ss') AS startTime,
            date_format((CASE WHEN endTime > (from_unixtime(timeSlot) + interval 30 minutes) THEN (from_unixtime(timeSlot) + interval 30 minutes) ELSE endTime END), 'yyyy-MM-dd HH:mm:ss') AS endTime,
            routingStatus,
            date_format(from_unixtime(timeSlot), 'yyyy-MM-dd HH:mm:ss') AS timeSlot, 
            (to_unix_timestamp(CASE WHEN endTime > (from_unixtime(timeSlot) + interval 30 minutes) THEN (from_unixtime(timeSlot) + interval 30 minutes) ELSE endTime END)
                - to_unix_timestamp((CASE WHEN startTime > from_unixtime(timeSlot) THEN startTime ELSE from_unixtime(timeSlot) END))
            ) AS timeDiff,
            pTableFlag
        FROM (
            SELECT
                userKey,
                startTime,
                endTime,
                routingStatus, 
                explode(sequence(
                    CAST((((floor((to_unix_timestamp(startTime) - 978267600) / 1800.0) * 30) * 60) + 978267600) AS long),
                    CAST((((floor((to_unix_timestamp(endTime) - 978267600) / 1800.0) * 30) * 60) + 978267600) AS long),
                    1800
                )) AS timeSlot,
                timeDiff,
                pTableFlag,
                timeZone
            FROM
                routing
            WHERE
                startTime < endTime
        )
        WHERE
            is_valid_date_udf(date_format((CASE WHEN startTime > from_unixtime(timeSlot) THEN startTime ELSE from_unixtime(timeSlot) END), 'yyyy-MM-dd HH:mm:ss'), timeZone)
   """)

    return df
