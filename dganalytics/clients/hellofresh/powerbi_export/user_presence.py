from dganalytics.utils.utils import get_spark_session, export_powerbi_csv, get_path_vars
from pyspark.sql import SparkSession
from dganalytics.connectors.gpc.gpc_utils import pb_export_parser, get_dbname, gpc_utils_logger
import os
import pandas as pd


def export_user_presence(spark: SparkSession, tenant: str, region: str):
    tenant_path, db_path, log_path = get_path_vars(tenant)
    user_timezone = pd.read_csv(os.path.join(tenant_path, 'data', 'config', 'User_Group_region_Sites.csv'), header=0)
    user_timezone = spark.createDataFrame(user_timezone)
    user_timezone.createOrReplaceTempView("user_timezone")

    dates = spark.sql("""
        SELECT 
            explode(date) AS date 
        FROM (
            SELECT
                sequence(add_months(current_date(), -12), current_date() + 2, interval 1 day) AS date
        )
    """)

    dates.createOrReplaceTempView("dates")

    pp = spark.sql(f"""
        SELECT  
            fp.userId AS UserKey, 
            fp.userId,
            from_utc_timestamp(fp.startTime, trim(ut.timeZone)) startTime,
            from_utc_timestamp(fp.endTime, trim(ut.timeZone)) endTime,
            fp.systemPresence,
            fp.organizationPresenceId,
            fp.presenceDefinitionName,
            fp.presenceDefinitionSystemPresence,
            fp.presenceDefinitionDeactivated,
            fp.presenceDefinitionPrimary,
            fp.presenceDefinitionLabel,
            'user_presence' pTableFlag,
            ut.region,
            startDate startDatePart
        FROM 
            gpc_hellofresh.fact_user_presence fp, user_timezone ut
        WHERE fp.userId = ut.userId
            AND CAST(from_utc_timestamp(fp.startTime, trim(ut.timeZone)) AS date) >= date_sub(current_date(), 35)
    """)

    pp.createOrReplaceTempView("presence")

    df = spark.sql("""
        SELECT
            pp.UserKey,
            pp.userId, 
            (case when cast(pp.startTime as date) < d.date then cast(d.date as timestamp) else pp.startTime end) startTime,
            (case when cast(pp.endTime as date) > d.date then (cast((d.date + 1) as timestamp) - interval 1 second) else pp.endTime end) endTime,
            systemPresence,
            organizationPresenceId,
            presenceDefinitionName,
            presenceDefinitionSystemPresence,
            presenceDefinitionDeactivated,
            presenceDefinitionPrimary,
            presenceDefinitionLabel,
            'user_presence' pTableFlag,
            region,
            startDatePart
        FROM
            presence pp, dates d 
        WHERE d.date BETWEEN cast(pp.startTime as date) AND cast(pp.endTime as date)
        AND CAST(pp.startTime AS date) >= date_sub(current_date(), 35)
    """)

    # Delete old data from the table for the last 5 days before inserting new data
    spark.sql(f"""
        DELETE FROM pbi_hellofresh.user_presence 
        WHERE startDatePart >= date_sub(current_date(), 35)
    """)

    # Write new data to the target table
    df.write.mode("append").saveAsTable("pbi_hellofresh.user_presence")

    return spark.sql(f"""SELECT * FROM pbi_hellofresh.user_presence
                     WHERE startDatePart > add_months(current_date(), -12)
                     {"AND region IN ('US', 'CA-HF', 'CP-CA', 'CA-CP','FA-HF')" if region == 'US' else " " }""")
