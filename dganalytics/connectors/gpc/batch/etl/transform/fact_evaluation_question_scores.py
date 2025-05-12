from pyspark.sql import SparkSession


def fact_evaluation_question_scores(spark: SparkSession, extract_date, extract_start_time, extract_end_time):
    evaluation_question_scores = spark.sql(f"""
                                    select distinct evaluationId,questionGroupId, qa.questionId,
                                    qa.answerId, qa.comments,
                                        qa.failedKillQuestion, qa.markedNA, qa.score, 
                                        conversationDatePart, sourceRecordIdentifier, soucePartition
                                            from (
                                    select id as evaluationId,
                                    questionGroupScores.questionGroupId, questionGroupScores.questionScores,
                                    date_format(conversationDate, 'yyyy-MM-dd') conversationDatePart,
                                    recordIdentifier as sourceRecordIdentifier,
                                    concat(extractDate, '|', extractIntervalStartTime, '|', extractIntervalEndTime) as soucePartition
                                        from raw_evaluations
                                        lateral view explode(answers.questionGroupScores) as questionGroupScores
                                        where extractDate = '{extract_date}'
                                        and  extractIntervalStartTime = '{extract_start_time}' and extractIntervalEndTime = '{extract_end_time}'
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
                        and source.conversationDatePart = target.conversationDatePart
                    WHEN MATCHED THEN
                        UPDATE SET *
                    WHEN NOT MATCHED THEN
                        INSERT *
            """)
