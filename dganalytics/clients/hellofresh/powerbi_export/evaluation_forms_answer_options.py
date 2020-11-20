from pyspark.sql import SparkSession

def export_evaluation_forms_answer_options(spark: SparkSession, tenant: str, region: str):

    df = spark.sql("""
            select 
a.evaluationFormId evaluationFormKey,
c.evaluationFormName evaluationFormName,
a.questionGroupId quesionGroupKey,
b.questionGroupName questionGroupName,
a.questionId questionKey,
a.answerOptionId answerOptionKey,
a.answerOptionText,
answerOptionValue
from gpc_hellofresh.dim_evaluation_form_answer_options a, gpc_hellofresh.dim_evaluation_form_question_groups b, gpc_hellofresh.dim_evaluation_forms c
where a.evaluationFormId = b.evaluationFormId
and a.questionGroupId = b.questionGroupId
and a.evaluationFormId  = c.evaluationFormId 
    """)

    return df
