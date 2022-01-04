from pyspark.sql import SparkSession

def fact_conversation_transcript_sentiments(spark: SparkSession, extract_date, extract_start_time, extract_end_time):
    transcript_sentiments = spark.sql(f"""
        SELECT DISTINCT conversationId,
                        communicationId,
                        mediaType,
                        sentiment.participant,
                        sentiment.phrase,
                        sentiment.sentiment,
                        sentiment.phraseIndex
        FROM (SELECT    conversationId,
                        communicationId,
                        mediaType,
                        EXPLODE(transcripts.analytics.sentiment) sentiment
                FROM (
                    SELECT  conversationId,
                            communicationId,
                            mediaType,
                            EXPLODE(transcripts) transcripts
                    FROM raw_speechandtextanalytics_transcript))
    """)

    transcript_sentiments.registerTempTable("transcript_sentiments")

    spark.sql("""DELETE FROM fact_conversation_transcript_sentiments
                WHERE conversationId IN (
                        SELECT DISTINCT conversationId
                        FROM transcript_sentiments
                    )
                """)

    spark.sql("""   INSERT INTO fact_conversation_transcript_sentiments
                    SELECT * FROM transcript_sentiments""")
