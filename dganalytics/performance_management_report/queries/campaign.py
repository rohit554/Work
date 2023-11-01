from dganalytics.utils.utils import exec_mongo_pipeline, get_active_org
from pyspark.sql.types import StructType, StructField, StringType, BooleanType
from datetime import datetime, timedelta
from pyspark.sql.functions import lower


schema = StructType([StructField('campaignId', StringType(), True),
                     StructField('endDate', StringType(), True), 
                     StructField('isActive', BooleanType(), True),
                     StructField('isDeleted', BooleanType(), True), 
                     StructField('name', StringType(), True),
                     StructField('orgId', StringType(), True), 
                     StructField('start_date', StringType(), True)])


def get_campaign(spark):
    extract_start_time = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    for tenant in get_active_org():
      pipeline = [
        {
          "$match": {
            "org_id": tenant,
            '$expr': {
                '$or': [
                    {
                        '$gte': [
                            '$creation_date', { '$date': extract_start_time }
                        ]
                    }, {
                        '$gte': [
                            '$modified_date', { '$date': extract_start_time }
                        ]
                    }
                ]
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
                "start_date": {
                    "$dateToString": {
                        "format": "%Y-%m-%d",
                        "date": "$start_date"
                    }
                },
                "endDate": {
                    "$dateToString": {
                        "format": "%Y-%m-%d",
                        "date": "$end_date"
                    }
                },
                "orgId" : "$org_id",
                "isActive" : "$is_active",
                "isDeleted":"$is_deleted"
            }
        }
      ]

      df = exec_mongo_pipeline(spark, pipeline, 'Campaign', schema)
      df = df.withColumn("orgId", lower(df["orgId"]))
      
      df.createOrReplaceTempView("campaign")

      spark.sql("""
      MERGE INTO dg_performance_management.campaign AS target
      USING campaign AS source
      ON target.orgId = source.orgId
      AND target.campaignId = source.campaignId
      WHEN NOT MATCHED THEN
        INSERT *        
      """)
    
