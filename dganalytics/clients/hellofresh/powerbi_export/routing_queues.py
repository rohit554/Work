from pyspark.sql import SparkSession
from dganalytics.utils.utils import get_path_vars
import os
import pandas as pd


def export_routing_queues(spark: SparkSession, tenant: str):
    tenant_path, db_path, log_path = get_path_vars(tenant)
    queue_mapping = pd.read_csv(os.path.join(tenant_path, 'data',
                                             'config', 'Queue_TimeZone_Mapping.csv'), header=0)
    # queue_mapping = spark.read.option("header", "true").csv(
    #    os.path.join('file:', tenant_path, 'data', 'config', 'Queue_TimeZone_Mapping.csv'))
    queue_mapping = spark.createDataFrame(queue_mapping)
    queue_mapping.registerTempTable("queue_mapping")

    df = spark.sql("""
            SELECT 
a.queueId queueKey, a.queueName as name, b.region, b.country
FROM gpc_hellofresh.dim_routing_queues a, queue_mapping b 
where a.queueName = b.queueName

    """)

    return df
