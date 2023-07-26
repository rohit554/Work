from pyspark.sql import SparkSession
from dganalytics.utils.utils import get_path_vars
import os
import pandas as pd


def export_routing_queues(spark: SparkSession, tenant: str, region: str):
    tenant_path, db_path, log_path = get_path_vars(tenant)
    queue_timezones = pd.read_json(os.path.join(tenant_path, 'data',
                                                'config', 'Queue_TimeZone_Mapping.json'))
    queue_timezones = pd.DataFrame(queue_timezones['values'].tolist())
    header = queue_timezones.iloc[0]
    queue_timezones = queue_timezones[1:]
    queue_timezones.columns = header

    queue_mapping = spark.createDataFrame(queue_timezones)
    queue_mapping.createOrReplaceTempView("queue_mapping")

    df = spark.sql("""
            SELECT 
              a.queueId queueKey, 
              a.queueName as name, 
              b.region, 
              b.country
          FROM 
              gpc_hellofresh.dim_routing_queues a, 
              queue_mapping b,
              gpc_hellofresh.dim_conversations c
          WHERE 
              a.queueName = b.queueName
              AND a.queueId = c.queueId
              AND CAST(from_utc_timestamp(c.conversationStart, trim(b.timeZone)) AS date) >= add_months(current_date(), -12)
              AND b.region {" = 'US'" if region == 'US' else " <> 'US'" }

    """)

    return df
