from dganalytics.utils.utils import get_spark_session
from dganalytics.connectors.gpc.gpc_utils import transform_parser, get_dbname
from delta.tables import DeltaTable

if __name__ == "__main__":
    tenant, run_id, extract_date = transform_parser()
    spark = get_spark_session(app_name="fact_evaluation_total_scores", tenant=tenant, default_db=get_dbname(tenant))

    evaluations = spark.sql(f"""
								select id as evaluationId, answers.totalCriticalScore, 		
		                    answers.totalNonCriticalScore, answers.totalScore
	            from raw_evaluations  where extractDate = '{extract_date}'
								""")
    DeltaTable.forName(spark, "fact_evaluation_total_scores").alias("target").merge(evaluations.coalesce(1).alias("source"),
                                                                         """source.evaluationId = target.evaluationId
			                """).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
