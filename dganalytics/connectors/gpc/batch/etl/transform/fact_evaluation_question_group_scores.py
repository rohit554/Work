from pyspark.sql import SparkSession


def fact_evaluation_question_group_scores(spark: SparkSession, extract_date: str):
    evaluation_question_group_scores = spark.sql(f"""
                                    select id as evaluationId, 
                            questionGroupScores.questionGroupId, questionGroupScores.markedNA, questionGroupScores.maxTotalCriticalScore,
                            questionGroupScores.maxTotalCriticalScoreUnweighted, questionGroupScores.maxTotalNonCriticalScore,
                            questionGroupScores.maxTotalNonCriticalScoreUnweighted, questionGroupScores.maxTotalScore,
                            questionGroupScores.maxTotalScoreUnweighted, questionGroupScores.totalCriticalScore,
                            questionGroupScores.totalCriticalScoreUnweighted, questionGroupScores.totalNonCriticalScore,
                            questionGroupScores.totalNonCriticalScoreUnweighted, questionGroupScores.totalScore, questionGroupScores.totalScoreUnweighted
                                from raw_evaluations
        lateral view explode(answers.questionGroupScores) as questionGroupScores  where extractDate = '{extract_date}'
                                    """)

    evaluation_question_group_scores.registerTempTable("evaluation_question_group_scores")
    spark.sql("""
                    merge into fact_evaluation_question_group_scores as target
                        usgin evaluation_question_group_scores as source
                        on source.evaluationId = target.evaluationId
                        and source.questionGroupId = target.questionGroupId
                    WHEN MATCHED THEN
                        UPDATE SET *
                    WHEN NOT MATCHED THEN
                        INSERT *
                """)
