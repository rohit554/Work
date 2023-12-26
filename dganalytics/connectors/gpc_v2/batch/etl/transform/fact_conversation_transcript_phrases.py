from pyspark.sql import SparkSession

def fact_conversation_transcript_phrases(spark: SparkSession, extract_date, extract_start_time, extract_end_time):
    transcript_sentiments = spark.sql(f"""
        SELECT conversationId,
                communicationId,
                transcriptId,
                language,
                phrase.text,
                phrase.stability,
                phrase.startTimeMs,
                phrase.duration.milliseconds,
                phrase.participantPurpose
        FROM (SELECT conversationId,
                  communicationId,
                  transcript.transcriptId,
                  transcript.language,
                  EXPLODE(transcript.phrases) phrase
          FROM (  SELECT conversationId,
                  communicationId,
                  EXPLODE(transcripts) transcript
            FROM ( SELECT conversationId,
                    communicationId,
                    transcripts,
                    row_number() OVER (PARTITION BY conversationId ORDER BY recordInsertTime DESC) RN
              FROM raw_speechandtextanalytics_transcript
              WHERE   extractDate = '{extract_date}'
                    AND  extractIntervalStartTime = '{extract_start_time}'
                    AND extractIntervalEndTime = '{extract_end_time}')
            WHERE RN = 1
          )
        )
    """)

    transcript_sentiments.createOrReplaceTempView("transcript_phrases")

    spark.sql("""DELETE FROM fact_conversation_transcript_phrases
                WHERE conversationId IN (
                        SELECT DISTINCT conversationId
                        FROM transcript_phrases
                    )
                """)

    spark.sql("""   INSERT INTO fact_conversation_transcript_phrases
                    SELECT * FROM transcript_phrases""")
