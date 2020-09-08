from pyspark.sql import SparkSession


def fact_evaluation_question_scores(spark: SparkSession, extract_date: str):
    evaluation_question_scores = spark.sql(f"""
                                    select distinct evaluationId,questionGroupId, qa.questionId, qa.answerId, qa.comments,
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
    evaluation_question_scores.registerTempTable("evaluation_question_scores")
    spark.sql("""
                merge into fact_evaluation_question_scores as target
                    using evaluation_question_scores as source
                    on source.evaluationId = target.evaluationId
                        and source.questionGroupId = target.questionGroupId 
                        and source.questionId = target.questionId 
                        and source.answerId = target.answerId
                    WHEN MATCHED THEN
                        UPDATE SET *
                    WHEN NOT MATCHED THEN
                        INSERT *
            """)
