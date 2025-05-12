from pyspark.sql import SparkSession

def fact_conversation_transcript_sentiments(spark: SparkSession, extract_date, extract_start_time, extract_end_time):
    transcript_sentiments = spark.sql(
    f"""
        SELECT DISTINCT conversationId,
                        communicationId,
                        mediaType,
                        sentiment.participant,
                        sentiment.phrase,
                        sentiment.sentiment,
                        sentiment.phraseIndex,conversationStartDate
        FROM (SELECT    conversationId,
                        communicationId,
                        mediaType,
                        EXPLODE(transcripts.analytics.sentiment) sentiment,conversationStartDate
                FROM (
                    SELECT  conversationId,
                            communicationId,
                            mediaType,
                            EXPLODE(transcripts) transcripts,
                            cast(from_unixtime(conversationStartTime / 1000) as date) conversationStartDate
                    FROM raw_speechandtextanalytics_transcript
               WHERE   extractDate = '{extract_date}'
                AND  extractIntervalStartTime = '{extract_start_time}'
                AND extractIntervalEndTime = '{extract_end_time}'))
    """
    )

    transcript_sentiments.createOrReplaceTempView("transcript_sentiments")

    spark.sql("""DELETE FROM fact_conversation_transcript_sentiments
                WHERE conversationId IN (
                        SELECT DISTINCT conversationId
                        FROM transcript_sentiments
                    )
                """)

    spark.sql("""   INSERT INTO fact_conversation_transcript_sentiments
                    SELECT * FROM transcript_sentiments""")
