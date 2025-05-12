from pyspark.sql import SparkSession

def fact_speechandtextanalytics(spark: SparkSession, extract_date, extract_start_time, extract_end_time):
    speechandtextanalytics_metrics = spark.sql(f"""
        SELECT  DISTINCT conversation.id AS conversationId,
                sentimentScore,
                sentimentTrend,
                participantMetrics.agentDurationPercentage,
                participantMetrics.customerDurationPercentage,
                participantMetrics.silenceDurationPercentage,
                participantMetrics.ivrDurationPercentage,
                participantMetrics.acdDurationPercentage,
				participantMetrics.overtalkDurationPercentage,
                participantMetrics.otherDurationPercentage,
                participantMetrics.overtalkCount
        FROM raw_speechandtextanalytics
        WHERE   extractDate = '{extract_date}'
                AND  extractIntervalStartTime = '{extract_start_time}'
                AND extractIntervalEndTime = '{extract_end_time}'
    """)

    speechandtextanalytics_metrics.createOrReplaceTempView("speechandtextanalytics_metrics")

    spark.sql("""DELETE FROM fact_speechandtextanalytics
                WHERE conversationId IN (
                        SELECT DISTINCT conversationId
                        FROM speechandtextanalytics_metrics
                    )
                """)

    spark.sql("""   INSERT INTO fact_speechandtextanalytics
                    SELECT * FROM speechandtextanalytics_metrics""")
