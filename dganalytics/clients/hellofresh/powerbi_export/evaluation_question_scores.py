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
            select /*+ BROADCAST(b) */ a.evaluationId evaluationKey,
b.conversationId conversationKey,
a.questionGroupId questionGroupKey,
a.answerId answerKey,
a.markedNA,
a.questionId questionKey,
a.score
from gpc_hellofresh.fact_evaluation_question_scores a, gpc_hellofresh.dim_evaluations b
where a.evaluationId = b.evaluationId
and a.conversationDatePart = b.conversationDatePart
    """)

    return df
