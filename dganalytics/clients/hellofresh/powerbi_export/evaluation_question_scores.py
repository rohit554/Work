from pyspark.sql import SparkSession
from dganalytics.utils.utils import get_path_vars
import os
import pandas as pd

def export_evaluation_question_scores(spark: SparkSession, tenant: str, region: str):
    tenant_path, db_path, log_path = get_path_vars(tenant)
    queue_mapping = pd.read_csv(os.path.join(tenant_path, 'data',
                                             'config', 'Queue_TimeZone_Mapping.csv'), header=0)
    # queue_mapping = spark.read.option("header", "true").csv(
    #    os.path.join('file:', tenant_path, 'data', 'config', 'Queue_TimeZone_Mapping.csv'))
    queue_mapping = spark.createDataFrame(queue_mapping)
    queue_mapping.registerTempTable("queue_mapping")

    df = spark.sql(f"""
            select a.evaluationId evaluationKey,
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
