from pyspark.sql import SparkSession


def fact_evaluation_question_group_scores(spark: SparkSession, extract_date, extract_start_time, extract_end_time):
    evaluation_question_group_scores = spark.sql(f"""
                                    select distinct id as evaluationId,
                            questionGroupScores.questionGroupId, questionGroupScores.markedNA,
                            questionGroupScores.maxTotalCriticalScore,
                            questionGroupScores.maxTotalCriticalScoreUnweighted,
                            questionGroupScores.maxTotalNonCriticalScore,
                            questionGroupScores.maxTotalNonCriticalScoreUnweighted, questionGroupScores.maxTotalScore,
                            questionGroupScores.maxTotalScoreUnweighted, questionGroupScores.totalCriticalScore,
                            questionGroupScores.totalCriticalScoreUnweighted, questionGroupScores.totalNonCriticalScore,
                            questionGroupScores.totalNonCriticalScoreUnweighted,
                            questionGroupScores.totalScore, questionGroupScores.totalScoreUnweighted,
                            date_format(conversationDate, 'yyyy-MM-dd') conversationDatePart,
                            recordIdentifier as sourceRecordIdentifier,
                            concat(extractDate, '|', extractIntervalStartTime, '|', extractIntervalEndTime) as soucePartition
                                from raw_evaluations
        lateral view explode(answers.questionGroupScores) as questionGroupScores  where extractDate = '{extract_date}'
                                    and  extractIntervalStartTime = '{extract_start_time}' and extractIntervalEndTime = '{extract_end_time}'""")

    evaluation_question_group_scores.createOrReplaceTempView(
        "evaluation_question_group_scores")
    spark.sql("""
                    merge into fact_evaluation_question_group_scores as target
                        using evaluation_question_group_scores as source
                        on source.evaluationId = target.evaluationId
                        and source.questionGroupId = target.questionGroupId
                        and source.conversationDatePart = target.conversationDatePart
                    WHEN MATCHED THEN
                        UPDATE SET *
                    WHEN NOT MATCHED THEN
                        INSERT *
                """)
