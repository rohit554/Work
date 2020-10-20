from dganalytics.utils.utils import exec_mongo_pipeline, delta_table_partition_ovrewrite
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import requests as rq
from pyspark.sql.functions import lit

schema = StructType([StructField('campaign_id', StringType(), True),
                     StructField('kpi', StringType(), True),
                     StructField('outcome_name', StringType(), True),
                     StructField('outcome_id', StringType(), True),
                     StructField('user_id', StringType(), True),
                     StructField('date', StringType(), True),
                     StructField('value', DoubleType(), True)])


def get_tp_kpi_raw_data(spark):
    holden_data = rq.get("https://holden.datagamz.com/api/auth/getKpiData?start_date=2020-07-01&end_date=2020-10-10&orgid=HOLDEN")

    holden_data = spark.read.json(spark._sc.parallelize(holden_data.json()['data'])).schema(schema).load()
    holden_data = holden_data.withColumn("orgId", lit('holden'))

    tp_orgs = ['bcp']
    for org in tp_orgs:
        tp_data = rq.get(f"https://teleperformance.datagamz.com/api/auth/getKpiData?start_date=2020-09-01&end_date=2020-10-10&orgid={org.upper()}")
        tp_data = spark.read.json(spark._sc.parallelize(tp_data.json()['data'])).schema(schema).load()
        tp_data = tp_data.withColumn("orgId", lit(org))

    df = holden_data.union(tp_data)

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
                """)
    '''
    df.coalesce(1).write.format("delta").mode("overwrite").partitionBy(
        'orgId').saveAsTable("dg_performance_management.tp_kpi_raw_data")
    '''
    delta_table_partition_ovrewrite(df, "dg_performance_management.tp_kpi_raw_data", ['orgId', 'date'])
