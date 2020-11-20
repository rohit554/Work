from pyspark.sql import SparkSession

def export_evaluation_forms_questions(spark: SparkSession, tenant: str, region: str):

    df = spark.sql("""
           select 
a.evaluationFormId evaluationFormKey,
c.evaluationFormName evaluationFormName,
a.questionGroupId quesionGroupKey,
b.questionGroupName questionGroupName,
a.questionId questionKey,
a.commentsRequired commentsRequired,
a.isCritical isCritical,
a.isKill isKill,
a.naEnabled naEnabled,
a.questionText as text
from gpc_hellofresh.dim_evaluation_form_questions a, gpc_hellofresh.dim_evaluation_form_question_groups b, gpc_hellofresh.dim_evaluation_forms c
where a.evaluationFormId = b.evaluationFormId
and a.questionGroupId = b.questionGroupId
and a.evaluationFormId  = c.evaluationFormId 
    """)

    return df
