from pyspark.sql import SparkSession
from dganalytics.utils.utils import get_path_vars
import os
import pandas as pd

def export_evaluation_question_scores(spark: SparkSession, tenant: str, region: str):
    tenant_path, db_path, log_path = get_path_vars(tenant)

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
            AND CAST(b.conversationDate AS DATE) >= add_months(current_date(), -12)
    """)

    return df
