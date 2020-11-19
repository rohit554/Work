
from pyspark.sql import SparkSession

def export_evaluation_question_scores(spark: SparkSession, tenant: str):

    df = spark.sql("""
            select a.evaluationId evaluationKey,
b.conversationId conversationKey,
a.questionGroupId questionGroupKey,
a.answerId answerKey,
a.markedNA,
a.questionId questionKey,
a.score
from gpc_hellofresh.fact_evaluation_question_scores a, gpc_hellofresh.dim_evaluations b
where a.evaluationId = b.evaluationId
    """)

    return df
