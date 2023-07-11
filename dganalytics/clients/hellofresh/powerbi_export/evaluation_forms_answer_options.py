from pyspark.sql import SparkSession

def export_evaluation_forms_answer_options(spark: SparkSession, tenant: str, region: str):

    df = spark.sql("""
            SELECT 
                a.evaluationFormId evaluationFormKey,
                c.evaluationFormName evaluationFormName,
                a.questionGroupId quesionGroupKey,
                b.questionGroupName questionGroupName,
                a.questionId questionKey,
                a.answerOptionId answerOptionKey,
                a.answerOptionText,
                answerOptionValue
            FROM    
                gpc_hellofresh.dim_evaluation_form_answer_options a, 
                gpc_hellofresh.dim_evaluation_form_question_groups b,   
                gpc_hellofresh.dim_evaluation_forms c
            WHERE 
                a.evaluationFormId = b.evaluationFormId
                AND a.questionGroupId = b.questionGroupId
                AND a.evaluationFormId  = c.evaluationFormId 
    """)

    return df
