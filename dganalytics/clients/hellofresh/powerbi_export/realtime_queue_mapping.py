from pyspark.sql import SparkSession
from dganalytics.utils.utils import get_path_vars
import os
import pandas as pd

def export_realtime_queue_mapping(spark: SparkSession, tenant: str, region: str):
	tenant_path, db_path, log_path = get_path_vars(tenant)
	queue_timezones = pd.read_json(os.path.join(tenant_path, 'data', 'config', 'Queue_TimeZone_Mapping_v2.json'))
	queue_timezones = pd.DataFrame(queue_timezones['values'].tolist())
	header = queue_timezones.iloc[0]
	queue_timezones = queue_timezones[1:]
	queue_timezones.columns = header

	queue_timezones = spark.createDataFrame(queue_timezones)
	queue_timezones = queue_timezones.withColumnRenamed("Country-Brand-Language", "country_brand_language")
	queue_timezones.createOrReplaceTempView("queue_timezones")

	queueHODashboard = pd.read_json(os.path.join(tenant_path, 'data', 'config', 'HO_Dashboard_Mapping.json'))

	idleQueues = []
	for queue in queueHODashboard['values'].tolist()[1:]:
		idleQueues.append([queue[0],queue[1].strip()])
		header = ['country_brand_language', 'idle_queue_name']

	idleQueuesDf = pd.DataFrame(idleQueues, columns = header)
	idleQueuesDf = spark.createDataFrame(idleQueuesDf)
	idleQueuesDf.createOrReplaceTempView("idleQueuesDf")

	idle_queue = spark.sql("""
	  SELECT	iq.country_brand_language as country_brand_language, 
			  rq.queueId as idle_queue_id 
	  FROM idleQueuesDf iq
	  INNER JOIN gpc_hellofresh.dim_routing_queues rq 
		  ON lower(iq.idle_queue_name) = lower(rq.queueName)
			""")

	idle_queue.createOrReplaceTempView("idle_queue")

	realtime_queues = spark.sql("""
		SELECT	qt.country,
				0 AS group_id,
				rq.queueId AS queue_id,
				qt.queueName AS queue_name,
				region,
				timeZone AS time_offset,
				qt.country_brand_language AS country_brand_language,
				iq.idle_queue_id
		FROM queue_timezones qt
		INNER JOIN gpc_hellofresh.dim_routing_queues rq 
			ON qt.queueName = rq.queueName
		INNER JOIN idle_queue iq
			ON iq.country_brand_language = qt.country_brand_language
		WHERE qt.country_brand_language IS NOT NULL
	""")

	realtime_queues.toPandas().to_csv(os.path.join(tenant_path, 'data', 'config', "realtime_queueMapping_v2.csv"), index=False)
	return realtime_queues