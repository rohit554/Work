from dganalytics.utils.utils import get_path_vars
from pyspark.sql import SparkSession
import os
import pandas as pd


def export_conversion_metrics_daily_summary(spark: SparkSession, tenant: str, region: str):

    tenant_path, db_path, log_path = get_path_vars(tenant)
    '''
    queue_timezones = pd.read_csv(os.path.join(tenant_path, 'data',
                                               'config', 'Queue_TimeZone_Mapping.csv'), header=0)
        '''
    queue_timezones = pd.read_json(os.path.join(tenant_path, 'data',
                                                'config', 'Queue_TimeZone_Mapping.json'))
    queue_timezones = pd.DataFrame(queue_timezones['values'].tolist())
    header = queue_timezones.iloc[0]
    queue_timezones = queue_timezones[1:]
    queue_timezones.columns = header

    queue_timezones = spark.createDataFrame(queue_timezones)
    queue_timezones.createOrReplaceTempView("queue_timezones")

    df = spark.sql(f"""
		SELECT
			CAST(from_utc_timestamp(a.intervalStart, trim(c.timeZone)) AS date) AS emitDate,
			date_format(from_utc_timestamp(a.intervalStart, trim(c.timeZone)), 'HH:mm:ss') as intervalStart,
			date_format(from_utc_timestamp(a.intervalEnd, trim(c.timeZone)), 'HH:mm:ss') as intervalEnd,
			conversationId,
			conversationStart,
			conversationEnd,
			originatingDirection

		FROM
			gpc_hellofresh.dim_conversations a,
			gpc_hellofresh.dim_routing_queues b,
			queue_timezones c
		WHERE
			a.queueId = b.queueId
			AND b.queueName = c.queueName
			AND c.region {" = 'US'" if region == 'US' else " <> 'US'" }
    """)

    return df
