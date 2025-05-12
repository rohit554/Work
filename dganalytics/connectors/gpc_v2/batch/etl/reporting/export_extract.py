from pyspark.sql.window import Window
from pyspark.sql.functions import lag, concat_ws, col

def get_dim_conversation(spark):
  df = spark.sql(f'''
      SELECT DISTiNCT conversationId,
                      conversationStart,
                      conversationEnd,
                      originatingDirection,
                      sessionStart,
                      sessionEnd,
                      queueId,
                      mediaType,
                      messageType,
                      agentId,
                      wrapUpCode
      FROM dim_conversations
  ''')
  
  window = Window.partitionBy("conversationId").orderBy("sessionStart")
  return df.withColumn("previous_queueId", lag("queueId", 1, None).over(window))

def get_speechandtextanalytics_topics(spark):
    df = spark.sql("""
        SELECT  DISTINCT id topicId,
                name,
                description,
                published,
                strictness,
                programsCount,
                tags,
                dialect,
                participants,
                phrasesCount
        FROM raw_speechandtextanalytics_topics
    """)

    return df.withColumn("tags", concat_ws(",",col("tags")))