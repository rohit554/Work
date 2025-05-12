from pyspark.sql import SparkSession

def fact_conversation_transcript_topics(spark: SparkSession, extract_date, extract_start_time, extract_end_time):
    transcript_topics = spark.sql(f"""
                    SELECT DISTINCT conversationId,
                    communicationId,
                    mediaType,
                    topics.participant,
                    topics.topicId,
                    topics.topicName,
                    topics.topicPhrase,
                    topics.transcriptPhrase,
                    topics.confidence
            FROM (SELECT  conversationId,
                    communicationId,
                    mediaType,
                    EXPLODE(transcripts.analytics.topics) topics
            FROM (
              SELECT  conversationId,
                      communicationId,
                      mediaType,
                      EXPLODE(transcripts) transcripts
              FROM raw_speechandtextanalytics_transcript
	      WHERE   extractDate = '{extract_date}'
                AND  extractIntervalStartTime = '{extract_start_time}'
                AND extractIntervalEndTime = '{extract_end_time}'))
    """)

    transcript_topics.createOrReplaceTempView("transcript_topics")

    spark.sql("""DELETE FROM fact_conversation_transcript_topics
                WHERE conversationId IN (
                        SELECT DISTINCT conversationId
                        FROM transcript_topics
                    )
                """)

    spark.sql("""   INSERT INTO fact_conversation_transcript_topics
                    SELECT * FROM transcript_topics""")
