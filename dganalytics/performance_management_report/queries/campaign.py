from dganalytics.utils.utils import exec_mongo_pipeline, delta_table_partition_ovrewrite, get_active_org
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DateType
from pyspark.sql.functions import lower


schema = StructType([StructField('campaignId', StringType(), True),
                     StructField('endDate', DateType(), True), 
                     StructField('isActive', BooleanType(), True),
                     StructField('isDeleted', BooleanType(), True), 
                     StructField('name', StringType(), True),
                     StructField('orgId', StringType(), True), 
                     StructField('start_date', DateType(), True)])


def get_campaign(spark):
  pipeline = [
    {
      "$match": {
        "org_id" :  {
          '$in' : get_active_org()
        },
        "is_active": True,
        "is_deleted": False
      } 
    },
    {
      "$project": {
          "_id": 0.0,
          "campaignId": "$_id",
          "name": 1.0,
          "start_date": "$start_date",
          "endDate": "$end_date",
          "orgId" : "$org_id",
          "isActive" : "$is_active",
          "isDeleted":"$is_deleted"
        }
    }
  ]

  df = exec_mongo_pipeline(spark, pipeline, 'Campaign', schema)
  df = df.withColumn("orgId", lower(df["orgId"]))
  
  delta_table_partition_ovrewrite(df, "dg_performance_management.campaign", ['orgId'])

