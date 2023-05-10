from dganalytics.utils.utils import get_spark_session, push_gamification_data, export_powerbi_csv
from dganalytics.connectors.gpc.gpc_utils import dg_metadata_export_parser, get_dbname, gpc_utils_logger
from pyspark.sql import SparkSession


def get_coles_data(spark: SparkSession, extract_date: str, org_id: str):
    back_days = 7
    df = spark.sql(f"""
        WITH UserDates AS (
            SELECT
            u.userId,
            date_format(D._date, 'dd-MM-yyyy') AS _date
            FROM
            gpc_salmatcolesonline.dim_users u
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
        FC AS (
            SELECT
            U.userId,
            U._date,
            SUM(E.tHeldComplete) AS tHeldComplete,
            SUM(E.nHeldComplete) AS nHeld,
            COUNT(DISTINCT E.conversationId) AS nHandle,
            SUM(E.tAcw) AS tAcw,
            SUM(E.nAcw) AS nAcw,
            COUNT(DISTINCT E.wrapUpCode, E.conversationId) AS wrapUpCodeCount
            FROM
            gpc_salmatcolesonline.fact_conversation_metrics E
            JOIN UserDates U ON U.userId = E.agentId
            AND date_format(
                from_utc_timestamp(E.emitDateTime, 'Australia/Sydney'),
                'dd-MM-yyyy'
            ) = U._date
            GROUP BY
            U.userId,
            _date
        ),
        FW AS (
            SELECT
            U.userId,
            U._date,
            SUM(D.adherenceScheduleSecs) AS adherenceScheduleSecs,
            SUM(D.exceptionDurationSecs) AS exceptionDurationSecs
            FROM
            gpc_salmatcolesonline.fact_wfm_day_metrics D
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
            FC.tHeldComplete AS tHeldComplete,
            FC.nHeld AS nHeld,
        FC.nHandle AS nHandle,
        FC.tAcw AS tAcw,
        FC.nAcw AS nAcw,
        FC.wrapUpCodeCount AS wrapUpCodeCount,
        FW.adherenceScheduleSecs AS adherenceScheduleSecs,
        FW.exceptionDurationSecs AS exceptionDurationSecs
        FROM
            UserDates UD
            LEFT JOIN FC ON FC.userId = UD.userId
            AND FC._date = UD._date
            LEFT JOIN FW ON FW.userId = UD.userId
            AND FW._date = UD._date
        WHERE
            NOT (
            FC.tHeldComplete IS NULL
            AND FC.nHandle IS NULL
            AND FC.tAcw IS NULL
            AND FC.nHeld IS NULL
            AND FC.nAcw IS NULL
            AND COALESCE(FC.wrapUpCodeCount, 0) = 0
            AND COALESCE(FW.adherenceScheduleSecs, 0) = 0
            AND COALESCE(FW.exceptionDurationSecs, 0) = 0
            )
    """)
    return df


if __name__ == "__main__":
    tenant, run_id, extract_date, org_id = dg_metadata_export_parser()
    db_name = get_dbname(tenant)
    app_name = "gpc_dg_metadata_colesonline_export"
    spark = get_spark_session(app_name, tenant, default_db=db_name)
    logger = gpc_utils_logger(tenant, app_name)
    try:
        logger.info("gpc_dg_metadata_colesonline_export")

        df = get_coles_data(spark, extract_date, org_id)
        df = df.drop_duplicates()
        push_gamification_data(
            df.toPandas(), 'SALMATCOLESONLINE', 'ProbecolesConnection')

    except Exception as e:
        logger.exception(e, stack_info=True, exc_info=True)
        raise
