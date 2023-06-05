from dganalytics.utils.utils import get_spark_session, push_gamification_data, export_powerbi_csv
from dganalytics.connectors.gpc.gpc_utils import dg_metadata_export_parser, get_dbname, gpc_utils_logger
from pyspark.sql.functions import from_utc_timestamp, col
from pyspark.sql.types import DateType
from pyspark.sql import SparkSession
from datetime import datetime, timedelta


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
            COUNT(DISTINCT E.conversationId) AS nHandle,
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
        FC_Voice AS (
            SELECT
            U.userId,
            U._date,
            (CASE WHEN COUNT(E.tHeldComplete) = 0 THEN null ELSE SUM(E.tHeldComplete) END) AS tHeldCompeleVoice,
            COUNT(DISTINCT E.conversationId) AS nHandleVoice,
            (CASE WHEN COUNT(E.tAcw) = 0 THEN null ELSE SUM(E.tAcw) END) AS tAcwVoice
            FROM
            gpc_salmatcolesonline.fact_conversation_metrics E
            JOIN UserDates U ON U.userId = E.agentId
            AND date_format(
                from_utc_timestamp(E.emitDateTime, 'Australia/Sydney'),
                'dd-MM-yyyy'
            ) = U._date
            WHERE E.mediaType = "voice"
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
        FC_Voice.tHeldCompeleVoice AS tHeldCompeleVoice,
        FC_Voice.nHandleVoice AS nHandleVoice,
        FC_Voice.tAcwVoice AS tAcwVoice,
        FC.nHandle AS nHandle,
        FC.tAcwVoice AS tAcwVoice,
        FC.wrapUpCodeCount AS wrapUpCodeCount,
        FW.adherenceScheduleSecs AS adherenceScheduleSecs,
        FW.exceptionDurationSecs AS exceptionDurationSecs
        FROM
            UserDates UD
            LEFT JOIN FC ON FC.userId = UD.userId
            AND FC._date = UD._date
            LEFT JOIN FC_Voice ON FC_Voice.userId = UD.userId
            AND FC_Voice._date = UD._date
            LEFT JOIN FW ON FW.userId = UD.userId
            AND FW._date = UD._date
        WHERE
            NOT (
            FC.nHandle IS NULL
            AND FC_Voice.tHeldCompeleVoice IS NULL 
            AND FC_Voice.nHandleVoice IS NULL
            AND FC_Voice.tAcwVoice IS NULL
            AND COALESCE(FC.wrapUpCodeCount, 0) = 0
            AND COALESCE(FW.adherenceScheduleSecs, 0) = 0
            AND COALESCE(FW.exceptionDurationSecs, 0) = 0
            )
    """)
    return df

def get_surveys(spark: SparkSession, extract_date: str):
    back_date = 20
    return spark.sql(f"""
        SELECT
            agentId as userId,
            date_format(conversationEnd, 'dd-MM-yyyy') Date,
            csat CSAT
        FROM
            gpc_salmatcolesonline.fact_conversation_survey
        where
            survey_initiated AND survey_completed
            AND csat is not NULL
            AND insertTimestamp > CAST('{extract_date}' AS DATE) - {back_date}
    """)

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
        surveys = get_surveys(spark, extract_date)
        surveys = surveys.drop_duplicates()
        push_gamification_data(
            surveys.toPandas(), 'SALMATCOLESONLINE', 'ProbeColesSurvey')
    except Exception as e:
        logger.exception(e, stack_info=True, exc_info=True)
        raise
