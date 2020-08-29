from pyspark.sql import SparkSession


def fact_evaluation_total_scores(spark: SparkSession, extract_date: str):
    evaluation_total_scores = spark.sql(f"""
                                    select id as evaluationId, answers.totalCriticalScore, 		
                                answers.totalNonCriticalScore, answers.totalScore
                    from raw_evaluations  where extractDate = '{extract_date}'
                                    """)
    evaluation_total_scores.registerTempTable("evaluation_total_scores")
    spark.sql("""
                merge into fact_evaluation_total_scores as target
                    using evaluation_total_scores as source
                        on source.evaluationId = target.evaluationId
                    WHEN MATCHED THEN
                        UPDATE SET *
                    WHEN NOT MATCHED THEN
                        INSERT *
                """)
