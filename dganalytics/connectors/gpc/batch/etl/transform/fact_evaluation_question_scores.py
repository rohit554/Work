from numpy.lib.shape_base import apply_along_axis
from dganalytics.utils.utils import get_spark_session
from dganalytics.connectors.gpc.gpc_utils import gpc_utils_logger, transform_parser, get_dbname
from delta.tables import DeltaTable

if __name__ == "__main__":
    tenant, run_id, extract_date = transform_parser()
    app_name = "fact_evaluation_question_scores"
    spark = get_spark_session(app_name=app_name, tenant=tenant, default_db=get_dbname(tenant))

    logger = gpc_utils_logger(tenant, app_name)
    try:
        logger.info("Upserting into fact_evaluation_question_scores")
        evaluations = spark.sql(f"""
                                    select evaluationId,questionGroupId, qa.questionId, qa.answerId, qa.comments,
                                        qa.failedKillQuestion, qa.markedNA, qa.score
                                            from (
                                    select id as evaluationId, 
                                    questionGroupScores.questionGroupId, questionGroupScores.questionScores
                                        from raw_evaluations
                                        lateral view explode(answers.questionGroupScores) as questionGroupScores
                                        where extractDate = '{extract_date}'
                                    )
                                    lateral view explode(questionScores) as qa  
                                    """)
        DeltaTable.forName(spark, "fact_evaluation_question_scores").alias("target").merge(evaluations.coalesce(1).alias("source"),
                                                                            """source.evaluationId = target.evaluationId
                            and source.questionGroupId = target.questionGroupId 
                            and source.questionId = target.questionId 
                            and source.answerId = target.answerId """).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    except Exception as e:
        logger.error(str(e))