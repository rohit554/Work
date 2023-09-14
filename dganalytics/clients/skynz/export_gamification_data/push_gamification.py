from dganalytics.utils.utils import get_spark_session, push_gamification_data, export_powerbi_csv
from dganalytics.connectors.gpc.gpc_utils import dg_metadata_export_parser, get_dbname, gpc_utils_logger
from pyspark.sql.functions import from_utc_timestamp, col
from pyspark.sql.types import DateType
from pyspark.sql import SparkSession
from datetime import datetime, timedelta


#inbound quality data
def inbound_quality(org_id):
    tenant = 'skynzib'
    app_name = f"{tenant}_evaluations"
    spark = get_spark_session(app_name=app_name, tenant = tenant)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)
    inbound_evaluations = spark.sql(f"""
                            WITH maxScore AS (
                            SELECT
                            questionGroupId,
                            questionGroupName,
                            questionId,
                            MAX(answerOption.value) AS maxScore
                            FROM (
                            SELECT
                                questionGroupId,
                                questionGroupName,
                                question.id AS questionId,
                                EXPLODE(question.answerOptions) AS answerOption
                            FROM (
                                SELECT
                                    questionGroup.id AS questionGroupId,
                                    questionGroup.name AS questionGroupName,
                                    EXPLODE(questionGroup.questions) AS question
                                FROM (
                                    SELECT EXPLODE(questionGroups) AS questionGroup
                                    FROM gpc_skynz.raw_evaluation_forms
                                    WHERE name LIKE '%IB Sales CC-RAW%'
                                )
                                WHERE questionGroup.name IN (
                                    'Step 1: Create Connection',
                                    'Step 5: Wrap'
                                )
                            )
                        )
                        GROUP BY
                            questionGroupId,
                            questionGroupName,
                            questionId
                    )

                    SELECT
                        DISTINCT
                        m.agentId AS userId,
                        m.assignedDate AS `Date`,
                        ts.totalScore AS Overall_Score,
                        `Step 1: Create Connection` AS `QA Score - Create Connection`,
                        `Step 5: Wrap` AS `QA Score - Wrap`
                    FROM (
                        SELECT
                            e.agentId,
                            e.assignedDate,
                            e.evaluationId,
                            ms.questionGroupName,
                            (SUM(eqs.score) * 100 / SUM(ms.maxScore)) AS groupScore
                        FROM gpc_skynz.fact_evaluation_question_scores eqs
                        JOIN maxScore ms 
                          ON eqs.questionId = ms.questionId
                        JOIN gpc_skynz.dim_evaluations e 
                          ON e.evaluationId = eqs.evaluationId
                        WHERE NOT eqs.markedNA
                        AND e.assignedDate >= '{start_date}' AND e.assignedDate <= '{end_date}'
                        GROUP BY e.agentId, e.assignedDate, e.evaluationId, ms.questionGroupId, ms.questionGroupName
                    ) AS m
                    JOIN gpc_skynz.fact_evaluation_total_scores ts 
                      ON m.evaluationId = ts.evaluationId
                    PIVOT (
                        SUM(COALESCE(groupScore, 0))
                        FOR questionGroupName IN (
                            'Step 1: Create Connection',
                            'Step 5: Wrap'
                        )
                    )

                        """)

    inbound_evaluations = inbound_evaluations.withColumn(
        "Date",
        date_format(
            from_utc_timestamp(
                col("Date").cast("timestamp"),
                "Pacific/Auckland"
            ),
            "MM-dd-yyyy HH:mm:ss"
        )
    )

    inbound_evaluations = inbound_evaluations.withColumn(
        "Date",
        col("Date").cast("string")
    )

    push_gamification_data(inbound_evaluations.toPandas(), org_id.upper(), 'SKYIB_QA_Connection')


#outbound quality data 
def outbound_quality(org_id):
    tenant = 'skynzob'
    app_name = f"{tenant}_evaluations"
    spark = get_spark_session(app_name=app_name, tenant = tenant)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)
    outbound_evaluations = spark.sql(f"""
                            WITH maxScore AS (
                            SELECT
                            questionGroupId,
                            questionGroupName,
                            questionId,
                            MAX(answerOption.value) AS maxScore
                              FROM (
                              SELECT
                                questionGroupId,
                                questionGroupName,
                                question.id AS questionId,
                                EXPLODE(question.answerOptions) AS answerOption
                                FROM (
                                SELECT
                                    questionGroup.id AS questionGroupId,
                                    questionGroup.name AS questionGroupName,
                                    EXPLODE(questionGroup.questions) AS question
                                FROM (
                                    SELECT EXPLODE(questionGroups) AS questionGroup
                                    FROM gpc_skynz.raw_evaluation_forms
                                    WHERE name LIKE '%OB Sales CC-RAW%'
                                )
                                WHERE questionGroup.name IN (
                                    'Step 2: Confirm Need',
                                    'Step 4.  Add Value'
                                )
                            )
                        )
                        GROUP BY
                            questionGroupId,
                            questionGroupName,
                            questionId
                    )

                    SELECT
                        m.agentId AS userId,
                        m.assignedDate AS `Date`,
                        ts.totalScore AS `QA Score`,
                        `Step 2: Confirm Need` AS `QA Score - Confirm Need`,
                        `Step 4.  Add Value` AS `QA Score - Add Value`
                        FROM (
                        SELECT
                            e.agentId,
                            e.assignedDate,
                            e.evaluationId,
                            ms.questionGroupName,
                            (SUM(eqs.score) * 100 / SUM(ms.maxScore)) AS groupScore
                        FROM gpc_skynz.fact_evaluation_question_scores eqs
                        JOIN maxScore ms 
                          ON eqs.questionId = ms.questionId
                        JOIN gpc_skynz.dim_evaluations e 
                          ON e.evaluationId = eqs.evaluationId
                        WHERE NOT eqs.markedNA
                        AND e.assignedDate >= '{start_date}' AND e.assignedDate <= '{end_date}'
                        GROUP BY e.agentId, e.assignedDate, e.evaluationId, ms.questionGroupId, ms.questionGroupName
                    ) AS m
                    JOIN gpc_skynz.fact_evaluation_total_scores ts 
                      ON m.evaluationId = ts.evaluationId
                    PIVOT (
                        SUM(COALESCE(groupScore, 0))
                        FOR questionGroupName IN (
                            'Step 2: Confirm Need',
                            'Step 4.  Add Value'
                        )
                    )

                        """)

    outbound_evaluations = outbound_evaluations.withColumn(
        "Date",
        date_format(
            from_utc_timestamp(
                col("Date").cast("timestamp"),
                "Pacific/Auckland"
            ),
            "MM-dd-yyyy HH:mm:ss"
        )
    )

    outbound_evaluations = outbound_evaluations.withColumn(
        "Date",
        col("Date").cast("string")
    )

    push_gamification_data(outbound_evaluations.toPandas(), org_id.upper(), 'SKYOB_QA_Connection')


#adherence_data
def adherence_metrics(org_id, back_days):
    tenant = "skynzib"
    app_name = f"{tenant}_adherence_metrics"
    spark = get_spark_session(app_name=app_name, tenant=tenant)

    adherence = spark.sql(f"""
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
                      AND COALESCE(FW.exceptionDurationSecs, 0) = 0
                      )
              """)

    push_gamification_data(adherence.toPandas(), org_id.upper(), 'SKYIB_Adherence_Connection')

if __name__ == "__main__":
    org_id_inbound = 'skynzib'
    org_id_outbound = 'skynzob'
    back_days = 3
    
    inbound_quality(org_id_inbound)
    outbound_quality(org_id_outbound)
    adherence_metrics(org_id_inbound, back_days)