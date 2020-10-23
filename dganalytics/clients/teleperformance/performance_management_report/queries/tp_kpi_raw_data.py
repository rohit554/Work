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
    holden_data = rq.get(
        f"https://holden.datagamz.com/api/auth/getKpiData?start_date={start_date}&orgid=HOLDEN")

    holden_data = spark.read.schema(schema).json(spark._sc.parallelize(
        holden_data.json()['data']))
    holden_data = holden_data.withColumn("orgId", lit('holden'))
    df = holden_data

    tp_orgs = ['bcp', 'bob']
    for org in tp_orgs:
        tp_data = rq.get(
            f"https://teleperformance.datagamz.com/api/auth/getKpiData?start_date={start_date}&orgid={org.upper()}")
        tp_data = spark.read.schema(schema).json(spark._sc.parallelize(
            tp_data.json()['data']))
        tp_data = tp_data.withColumn("orgId", lit(org))
        df = df.union(tp_data)

    # df = holden_data.union(tp_data)

    df.registerTempTable("tp_kpi_raw_data")
    df = spark.sql("""
                    select  campaign_id as campaignId,
                            kpi as kpi,
                            outcome_name as outcomeName,
                            outcome_id as outcomeId,
                            user_id as userId,
                            cast(date as date) as date,
                            value as value,
                            orgId as orgId
                    from tp_kpi_raw_data
                    where value is not null
                """)
    '''
    df.coalesce(1).write.format("delta").mode("overwrite").partitionBy(
        'orgId').saveAsTable("dg_performance_management.tp_kpi_raw_data")
    '''
    delta_table_partition_ovrewrite(
        df, "dg_performance_management.tp_kpi_raw_data", ['orgId', 'date'])
