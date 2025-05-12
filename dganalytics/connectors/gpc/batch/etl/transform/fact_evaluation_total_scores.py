from pyspark.sql import SparkSession


def fact_evaluation_total_scores(spark: SparkSession, extract_date, extract_start_time, extract_end_time):
    evaluation_total_scores = spark.sql(f"""
                                    select distinct id as evaluationId, answers.totalCriticalScore,
                                answers.totalNonCriticalScore, answers.totalScore,
                                date_format(conversationDate, 'yyyy-MM-dd') conversationDatePart,
                                recordIdentifier as sourceRecordIdentifier,
                            concat(extractDate, '|', extractIntervalStartTime, '|', extractIntervalEndTime) as soucePartition
                    from raw_evaluations  where extractDate = '{extract_date}'
                    and  extractIntervalStartTime = '{extract_start_time}' and extractIntervalEndTime = '{extract_end_time}'
                                    """)
    evaluation_total_scores.registerTempTable("evaluation_total_scores")
    spark.sql("""
                merge into fact_evaluation_total_scores as target
                    using evaluation_total_scores as source
                        on source.evaluationId = target.evaluationId
                        and source.conversationDatePart = target.conversationDatePart
                    WHEN MATCHED THEN
                        UPDATE SET *
                    WHEN NOT MATCHED THEN
                        INSERT *
                """)
