from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from dganalytics.utils.utils import exec_mongo_pipeline, get_active_org
from datetime import datetime, timedelta
from pyspark.sql.functions import lower

schema = StructType([
    StructField('userId', StringType(), True),
    StructField('jobName', StringType(), True), 
    StructField('auditFile', StringType(), True), 
    StructField('startDate', StringType(), True),
    StructField('runID', StringType(), True), 
    StructField('fileName', StringType(), True), 
    StructField('status', StringType(), True), 
    StructField('entityName', StringType(), True), 
    StructField('endDate', StringType(), True), 
    StructField('message', StringType(), True),
    StructField('recordInserted', IntegerType(), True),
    StructField('orgId', StringType(), True),
])


def get_data_upload_audit_log(spark):
  extract_start_time = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
  for tenant in get_active_org():
    data_upload_audit_pipeline = [
          {
            "$match": {
                "org_id": tenant,
                "start_date":{
                        '$gte': { '$date': extract_start_time }
                    }
            }
          }, 
          {
            "$project": {
                "userId" : "$user_id",
                "jobName" : "$job_name", 
                "auditFile" : "$audit_file", 
                "startDate" : {
                    "$dateToString": {
                        "format": "%Y-%m-%dT%H:%M:%SZ",
                        "date": "$start_date"
                    }
                },
                "endDate" : {
                    "$dateToString": {
                        "format": "%Y-%m-%dT%H:%M:%SZ",
                        "date": "$end_date"
                    }
                }, 
                "runID" : 1, 
                "fileName" : "$file_name", 
                "status" : 1, 
                "entityName" : "$entity_name",
                "message":1,
                "recordInserted" : "$record_inserted",
                "orgId": "$org_id"
            }
        }]

    data_upload_logs = exec_mongo_pipeline(spark, data_upload_audit_pipeline, 'ETL_Audit_Log', schema)
    data_upload_logs = data_upload_logs.withColumn("orgId", lower(data_upload_logs["orgId"]))
    
    data_upload_logs.createOrReplaceTempView("data_upload_audit_log")
    spark.sql("""
      MERGE INTO dg_performance_management.data_upload_audit_log AS target
      USING data_upload_audit_log AS source
      ON target.orgId = source.orgId
      AND target.userId = source.userId
      AND target.runID = source.runID
      WHEN MATCHED THEN
                UPDATE SET *
      WHEN NOT MATCHED THEN
        INSERT *        
      """)