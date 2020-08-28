from dganalytics.utils.utils import get_spark_session
from dganalytics.connectors.gpc.gpc_utils import gpc_utils_logger, transform_parser, get_dbname
from delta.tables import DeltaTable

if __name__ == "__main__":
    tenant, run_id, extract_date = transform_parser()
    app_name = "dim_evaluations"
    spark = get_spark_session(app_name=app_name, tenant=tenant, default_db=get_dbname(tenant))

    logger = gpc_utils_logger(tenant, app_name)
    try:
        logger.info("Upserting into dim_evaluations")
        evaluations = spark.sql(f"""
                                    select id as evaluationId, evaluator.id as evaluatorId, agent.id as agentId, conversation.id as conversationId, 
                                        evaluationForm.id as evaluationFormId, status, assignedDate, releaseDate,
                                        changedDate, conversationDate, mediaType[0] as mediaType, 
                                        agentHasRead, answers.anyFailedKillQuestions, answers.comments, 
                                        evaluationForm.name as evaluationFormName, 
                                        evaluationForm.published as evaluationFormPublished, neverRelease,
                                        resourceType, cast(assignedDate as date) as assignedDatePart
                                    from raw_evaluations  where extractDate = '{extract_date}'
                                    """)
        DeltaTable.forName(spark, "dim_evaluations").alias("target").merge(evaluations.coalesce(1).alias("source"),
                                                                            """source.evaluationId = target.evaluationId
                                """).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    except Exception as e:
        logger.error(str(e))