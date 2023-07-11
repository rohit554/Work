from pyspark.sql import SparkSession
from dganalytics.utils.utils import get_path_vars
import os
import pandas as pd

def export_evaluation_question_scores(spark: SparkSession, tenant: str, region: str):
    tenant_path, db_path, log_path = get_path_vars(tenant)
    queue_timezones = pd.read_json(os.path.join(tenant_path, 'data',
                                                'config', 'Queue_TimeZone_Mapping.json'))
    queue_timezones = pd.DataFrame(queue_timezones['values'].tolist())
    header = queue_timezones.iloc[0]
    queue_timezones = queue_timezones[1:]
    queue_timezones.columns = header

    queue_mapping = spark.createDataFrame(queue_timezones)
    queue_mapping.createOrReplaceTempView("queue_mapping")

    df = spark.sql(f"""
        SELECT /*+ BROADCAST(b) */
            a.evaluationId AS evaluationKey,
            b.conversationId AS conversationKey,
            a.questionGroupId AS questionGroupKey,
            a.answerId AS answerKey,
            a.markedNA,
            a.questionId AS questionKey,
            a.score
        FROM
            gpc_hellofresh.fact_evaluation_question_scores a,
            gpc_hellofresh.dim_evaluations b
        WHERE
            a.evaluationId = b.evaluationId
            AND a.conversationDatePart = b.conversationDatePart
    """)

    return df
