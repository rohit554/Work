from dganalytics.utils.utils import exec_mongo_pipeline, delta_table_partition_ovrewrite
from pyspark.sql.types import StructType, StructField, StringType, FloatType
import requests as rq
from pyspark.sql.functions import lit
from datetime import datetime, timedelta

schema = StructType([StructField('campaign_id', StringType(), True),
                     StructField('kpi', StringType(), True),
                     StructField('outcome_name', StringType(), True),
                     StructField('outcome_id', StringType(), True),
                     StructField('user_id', StringType(), True),
                     StructField('date', StringType(), True),
                     StructField('value', FloatType(), True)])


def get_tp_kpi_raw_data(spark):
    start_date = (datetime.utcnow() - timedelta(days=15)).strftime("%Y-%m-%d")

    # tp_orgs = ['bcp', 'bob']
    tp_orgs = spark.sql("select distinct upper(orgId) as orgId from dg_performance_management.campaign").toPandas()['orgId'].tolist()
    for org in tp_orgs:
        tp_data = rq.get(
            f"https://teleperformance.datagamz.com/api/auth/getKpiData?start_date={start_date}&orgid={org.upper()}")
        df = spark.read.schema(schema).json(spark._sc.parallelize(
            tp_data.json()['data']))
        df = df.withColumn("orgId", lit(org))

        df.registerTempTable("tp_kpi_raw_data")
        df = spark.sql("""
                        select  distinct campaign_id as campaignId,
                                kpi as kpi,
                                outcome_name as outcomeName,
                                outcome_id as outcomeId,
                                user_id as userId,
                                cast(date as date) as date,
                                value as value,
                                lower(orgId) as orgId
                        from tp_kpi_raw_data
                        where value is not null
                    """)
        '''
        df.coalesce(1).write.format("delta").mode("overwrite").partitionBy(
            'orgId').saveAsTable("dg_performance_management.tp_kpi_raw_data")
        '''
        delta_table_partition_ovrewrite(
            df, "dg_performance_management.tp_kpi_raw_data", ['orgId', 'date'])
