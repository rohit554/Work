from dganalytics.utils.utils import get_spark_session
from dganalytics.connectors.gpc.gpc_utils import parser, get_dbname
from delta.tables import DeltaTable

if __name__ == "__main__":
    tenant, run_id, extract_date = parser()
    spark = get_spark_session(app_name="fact_evaluation_question_group_scores", tenant=tenant, default_db=get_dbname(tenant))

    evaluations = spark.sql(f"""
								select id as evaluationId, 
                        questionGroupScores.questionGroupId, questionGroupScores.markedNA, questionGroupScores.maxTotalCriticalScore,
                        questionGroupScores.maxTotalCriticalScoreUnweighted, questionGroupScores.maxTotalNonCriticalScore,
                        questionGroupScores.maxTotalNonCriticalScoreUnweighted, questionGroupScores.maxTotalScore,
                        questionGroupScores.maxTotalScoreUnweighted, questionGroupScores.totalCriticalScore,
                        questionGroupScores.totalCriticalScoreUnweighted, questionGroupScores.totalNonCriticalScore,
                        questionGroupScores.totalNonCriticalScoreUnweighted, questionGroupScores.totalScore, questionGroupScores.totalScoreUnweighted
                            from gpc_test.raw_evaluations
	lateral view explode(answers.questionGroupScores) as questionGroupScores  where extractDate = '{extract_date}'
								""")
    DeltaTable.forName(spark, "fact_evaluation_question_group_scores").alias("target").merge(evaluations.coalesce(1).alias("source"),
                                                                         """source.evaluationId = target.evaluationId
			               and source.questionGroupId = target.questionGroupId """).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
