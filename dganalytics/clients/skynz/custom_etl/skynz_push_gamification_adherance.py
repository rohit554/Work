from dganalytics.utils.utils import get_spark_session, push_gamification_data, export_powerbi_csv
from dganalytics.connectors.gpc.gpc_utils import dg_metadata_export_parser, get_dbname, gpc_utils_logger
from pyspark.sql.functions import from_utc_timestamp, col
from pyspark.sql.types import DateType
from pyspark.sql import SparkSession
from datetime import datetime, timedelta


def get_sky_adherence_IB_data(spark: SparkSession, extract_date: str, org_id: str):
    back_days = 3
    df = spark.sql(f"""
        WITH UserDates AS (
            SELECT
            u.userId,
            date_format(D._date, 'dd-MM-yyyy') AS _date
            FROM
            gpc_skynz.dim_users u
            CROSS JOIN (
                SELECT
                explode(
                    sequence(
                    CAST('{extract_date}' AS DATE) - {back_days},
                    CAST('{extract_date}' AS DATE),
                    interval 1 day
                    )
                ) _date
            ) AS D
            WHERE
            u.state = 'active'
        ),
        FW AS (
            SELECT
            U.userId,
            U._date,
            SUM(D.adherenceScheduleSecs) AS adherenceScheduleSecs,
            SUM(D.exceptionDurationSecs) AS exceptionDurationSecs
            FROM
            gpc_skynz.fact_wfm_day_metrics D
            JOIN UserDates U ON U.userId = D.userId
            AND date_format(
                from_utc_timestamp(D.startDate, 'Australia/Sydney'),
                'dd-MM-yyyy'
            ) = U._date
            GROUP BY
            U.userId,
            _date
        )
        SELECT
        UD.userId,
        UD._date AS Date,
        FW.adherenceScheduleSecs AS adherenceScheduleSecs,
        FW.exceptionDurationSecs AS exceptionDurationSecs
        FROM
            UserDates UD
            LEFT JOIN FW ON FW.userId = UD.userId
            AND FW._date = UD._date
        WHERE
            NOT (
            COALESCE(FW.adherenceScheduleSecs, 0) = 0
            AND COALESCE(FW.exceptionDurationSecs, 0) = 0
            )
    """)
    return df


if __name__ == "__main__":
    tenant, run_id, extract_date, org_id = dg_metadata_export_parser()
    db_name = get_dbname(tenant)
    app_name = "gpc_dg_metadata_adherence_export"
    spark = get_spark_session(app_name, tenant, default_db=db_name)
    logger = gpc_utils_logger(tenant, app_name)
    try:
        logger.info("gpc_dg_metadata_adherence_export")
        df = get_sky_adherence_IB_data(spark, extract_date)
        df = df.drop_duplicates()
        push_gamification_data(
            df.toPandas(), 'SKYNZIB', 'SKYIB_Adherence_Connection')
    except Exception as e:
        logger.exception(e, stack_info=True, exc_info=True)
        raise
