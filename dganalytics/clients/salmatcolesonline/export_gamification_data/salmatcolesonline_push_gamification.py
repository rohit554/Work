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
            COUNT(DISTINCT E.conversationId) AS nHandle,
            COUNT(DISTINCT E.wrapUpCode, E.conversationId) AS wrapUpCodeCount
            FROM
            gpc_salmatcolesonline.fact_conversation_metrics E
            JOIN UserDates U 
                ON U.userId = E.agentId
                  AND date_format(from_utc_timestamp(E.emitDateTime, 'Australia/Sydney'),'dd-MM-yyyy') = U._date
            GROUP BY
            U.userId,_date
        ),
        FC_Voice AS (
            SELECT
            U.userId,
            U._date,
            (CASE WHEN COUNT(E.tHeldComplete) = 0 THEN null ELSE SUM(COALESCE(E.tHeldComplete, 0)) END) AS tHeldCompleteVoice,
            COUNT(DISTINCT E.conversationId) AS nHandleVoice,
            (CASE WHEN COUNT(E.tAcw) = 0 THEN null ELSE SUM(COALESCE(E.tAcw, 0)) END) AS tAcwVoice
            FROM
            gpc_salmatcolesonline.fact_conversation_metrics E
            JOIN UserDates U 
                ON U.userId = E.agentId
                  AND date_format(from_utc_timestamp(E.emitDateTime, 'Australia/Sydney'),'dd-MM-yyyy') = U._date
            WHERE E.mediaType = "voice"
            GROUP BY
            U.userId,_date
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
            AND date_format(from_utc_timestamp(D.startDate, 'Australia/Sydney'),'dd-MM-yyyy') = U._date
            GROUP BY
            U.userId,_date
        )
        SELECT
        UD.userId,
        UD._date AS Date,
        FC_Voice.tHeldCompleteVoice AS tHeldCompleteVoice,
        FC_Voice.nHandleVoice AS nHandleVoice,
        FC_Voice.tAcwVoice AS tAcwVoice,
        FC.nHandle AS nHandle,
        FC.wrapUpCodeCount AS wrapUpCodeCount,
        FW.adherenceScheduleSecs AS adherenceScheduleSecs,
        FW.exceptionDurationSecs AS exceptionDurationSecs
        FROM
            UserDates UD
            LEFT JOIN FC 
                ON FC.userId = UD.userId
                  AND FC._date = UD._date
            LEFT JOIN FC_Voice 
                ON FC_Voice.userId = UD.userId
                  AND FC_Voice._date = UD._date
            LEFT JOIN FW 
                ON FW.userId = UD.userId
                  AND FW._date = UD._date
        WHERE
            NOT (
            FC.nHandle IS NULL
            AND FC_Voice.tHeldCompleteVoice IS NULL 
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
        WHERE
            survey_initiated AND survey_completed
            AND csat is not NULL
            AND insertTimestamp > CAST('{extract_date}' AS DATE) - {back_date}
    """)

def get_quality(spark: SparkSession, extract_date: str):
    back_date = 7
    return spark.sql(f"""
        SELECT b.agentId as userId,
          DATE_FORMAT(b.conversationDate, 'dd-MM-yyyy') as Date,
          LEAST( avg(c.totalScore)*100 / avg(c.maxTotalScore),100) as `QA Score`
          FROM gpc_salmatcolesonline.dim_evaluations b 
          LEFT JOIN gpc_salmatcolesonline.fact_evaluation_question_group_scores c ON c.evaluationId = b.evaluationId
          LEFT JOIN gpc_salmatcolesonline.fact_evaluation_question_scores d ON d.evaluationId = c.evaluationId
          WHERE b.conversationDate > CAST('{extract_date}' AS DATE) - {back_date}
          and c.markedNA ='false'
          and d.failedKillQuestion = 'false'
          GROUP BY b.agentId,DATE_FORMAT(b.conversationDate, 'dd-MM-yyyy')
    """)

def get_conversation(spark: SparkSession, extract_date: str):
    back_date = 7
    return spark.sql(f"""
        SELECT DISTINCT 
          agentId as userId, 
          DATE_FORMAT(emitDate, 'dd-MM-yyyy') as Date, 
          avg(tHandle) / avg(nHandle) AS AHT,
          sum(nTalkComplete) AS `No of Calls`,
          avg(tTalkComplete ) AS `Talk Time`
        FROM gpc_salmatcolesonline.fact_conversation_metrics  
        WHERE emitDate > DATE_SUB(CAST('{extract_date}' AS DATE), {back_date})
        GROUP BY agentId, DATE_FORMAT(emitDate, 'dd-MM-yyyy')
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
        quality = get_quality(spark, extract_date)
        quality = quality.drop_duplicates()
        push_gamification_data(
            quality.toPandas(), 'SALMATCOLESONLINE', 'ProbeQualityConnection')
        conversation = get_conversation(spark, extract_date)
        conversation = conversation.drop_duplicates()
        push_gamification_data(
            conversation.toPandas(), 'SALMATCOLESONLINE', 'ProbeConversationConnection')
    except Exception as e:
        logger.exception(e, stack_info=True, exc_info=True)
        raise