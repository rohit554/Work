from pyspark.sql.window import Window
from pyspark.sql.functions import lag

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