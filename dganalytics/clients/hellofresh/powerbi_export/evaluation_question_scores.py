from dganalytics.utils.utils import get_spark_session, export_powerbi_csv, get_path_vars
from pyspark.sql import SparkSession
from dganalytics.connectors.gpc.gpc_utils import pb_export_parser, get_dbname, gpc_utils_logger
import os


def export_evaluation_question_scores(spark: SparkSession, tenant: str):

    tenant_path, db_path, log_path = get_path_vars(tenant)
    df = spark.sql("""
            select evaluationId evaluationKey, evaluationId ,
questionGroupId questionGroupKey, questionGroupId, answerId answerKey, answerId, comments, 
failedKillQuestion, markedNA, questionId questionKey, questionId, score
from gpc_hellofresh.fact_evaluation_question_scores 
    """)

    return df
