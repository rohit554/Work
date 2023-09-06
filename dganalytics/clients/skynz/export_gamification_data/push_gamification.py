from dganalytics.utils.utils import get_spark_session, push_gamification_data, export_powerbi_csv
from dganalytics.connectors.gpc.gpc_utils import dg_metadata_export_parser, get_dbname, gpc_utils_logger
from pyspark.sql.functions import from_utc_timestamp, col
from pyspark.sql.types import DateType
from pyspark.sql import SparkSession
from datetime import datetime, timedelta


if __name__ == "__main__":
    org_id = 'skynzib'
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
                    CAST(current_date() AS DATE) - {back_days},
                    CAST(current_date() AS DATE),
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
                from_utc_timestamp(D.startDate, 'Pacific/Auckland'),
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
            )
    """)

    push_gamification_data(df.toPandas(), org_id.upper(), 'SKYIB_Adherence_Connection')
