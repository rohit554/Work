from dganalytics.utils.utils import get_spark_session
from dganalytics.connectors.gpc.gpc_utils import parser, get_dbname
from delta.tables import DeltaTable

if __name__ == "__main__":
    tenant, run_id, extract_date = parser()
    spark = get_spark_session(app_name="dim_evaluations", tenant=tenant, default_db=get_dbname(tenant))

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
