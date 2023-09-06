from dganalytics.utils.utils import get_spark_session, push_gamification_data, export_powerbi_csv
from dganalytics.connectors.gpc.gpc_utils import dg_metadata_export_parser, get_dbname, gpc_utils_logger
from pyspark.sql.functions import from_utc_timestamp, col
from pyspark.sql.types import DateType
from pyspark.sql import SparkSession
from datetime import datetime, timedelta


if __name__ == "__main__":
    org_id = 'skynzib'
    df = spark.sql(f"""
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
                WHERE name LIKE 'IB Sales CC-RAW '
                OR name = 'OB Sales CC-RAW'
            )
            WHERE questionGroup.name IN (
                'Step 1: Create Connection',
                'Step 5: Wrap',
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
            m.assignedDate,
            `Step 1: Create Connection` AS `QA Score - Create Connection`,
            `Step 2: Confirm Need` AS `QA Score - Confirm Need`,
            `Step 4.  Add Value` AS `QA Score - Add Value`,
            `Step 5: Wrap` AS `QA Score - Wrap`,
            ts.totalScore AS Overall_Score
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
            GROUP BY e.agentId, e.assignedDate, e.evaluationId, ms.questionGroupId, ms.questionGroupName
        ) AS m
        JOIN gpc_skynz.fact_evaluation_total_scores ts 
        ON m.evaluationId = ts.evaluationId
        PIVOT (
            SUM(COALESCE(groupScore, 0))
            FOR questionGroupName IN (
                'Step 1: Create Connection',
                'Step 2: Confirm Need',
                'Step 4.  Add Value',
                'Step 5: Wrap'
            )
        )

    """)

    push_gamification_data(df.toPandas(), org_id.upper(), 'SKYIB_Adherence_Connection')
