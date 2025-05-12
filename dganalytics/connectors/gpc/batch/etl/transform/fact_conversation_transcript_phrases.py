from pyspark.sql import SparkSession

def fact_conversation_transcript_phrases(spark: SparkSession, extract_date, extract_start_time, extract_end_time):
    transcript_sentiments = spark.sql(f"""
        
        SELECT a.conversationId,
                a.communicationId,
                a.transcriptId,
                a.language,
                a.phrase.text,
                a.phrase.stability,
                a.phrase.startTimeMs,
                a.phrase.duration.milliseconds,
                a.phrase.participantPurpose,
                a.conversationStartDate,
                b.sentiment
        FROM (SELECT conversationId,
                  communicationId,
                  transcript.transcriptId,
                  transcript.language,
                  EXPLODE(transcript.phrases) phrase,conversationStartDate
          FROM (  SELECT conversationId,
                  communicationId,
                  EXPLODE(transcripts) transcript,conversationStartDate
            FROM ( SELECT conversationId,
                    communicationId,
                    transcripts,
                    cast(from_unixtime(conversationStartTime / 1000) as date) conversationStartDate,
                    row_number() OVER (PARTITION BY conversationId ORDER BY recordInsertTime DESC) RN
              FROM raw_speechandtextanalytics_transcript
              WHERE   extractDate = '{extract_date}'
                    AND  extractIntervalStartTime = '{extract_start_time}'
                    AND extractIntervalEndTime = '{extract_end_time}')
            WHERE RN = 1
          )
        ) a
        left join fact_conversation_transcript_sentiments b
          on a.conversationId = b.conversationId
          and a.phrase.participantPurpose = b.participant
          AND LOWER(TRIM(REGEXP_REPLACE(a.phrase.text, '[^a-zA-Z0-9]', ''))) = LOWER(TRIM(REGEXP_REPLACE(b.phrase, '[^a-zA-Z0-9]', '')))

    """)

    transcript_sentiments.createOrReplaceTempView("transcript_phrases")

    spark.sql("""MERGE INTO fact_conversation_transcript_phrases target
                USING (
                    SELECT DISTINCT conversationId FROM transcript_phrases
                ) source
                ON target.conversationId = source.conversationId
                WHEN MATCHED THEN DELETE
                """)

    spark.sql("""   INSERT INTO fact_conversation_transcript_phrases
                    SELECT * FROM transcript_phrases""")
