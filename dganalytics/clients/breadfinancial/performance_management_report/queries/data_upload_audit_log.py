from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from dganalytics.utils.utils import exec_mongo_pipeline, delta_table_partition_ovrewrite
from dganalytics.clients.breadfinancial.utils import exec_mongo_pipeline

schema = StructType([
    StructField('user_id', StructType([StructField('oid', StringType(), True)]), True),
    StructField('job_name', StringType(), True), 
    StructField('audit_file', StringType(), True), 
    StructField('start_date', StringType(), True),
    StructField('runID', StringType(), True), 
    StructField('file_name', StringType(), True), 
    StructField('status', StringType(), True), 
    StructField('entity_name', StringType(), True), 
    StructField('end_date', StringType(), True), 
    StructField('message', StringType(), True),
    StructField('record_inserted', IntegerType(), True),
    StructField('org_id', StringType(), True),
])

org_timezone_schema = StructType([
    StructField('org_id', StringType(), True),
    StructField('timezone', StringType(), False)])


def get_data_upload_audit_log(spark):
    org_timezone_pipeline = [{
        "$match": {
            "$expr": {
                "$and": [
                {
                    "$eq": ["$type", "Organisation"]
                }, {
                    "$eq": ["$is_active", True]
                }, {
                    "$eq": ["$is_deleted", False]
                }]
            }
        }
    }, {
        "$project": {
            "org_id": 1,
            "timezone": {
                "$ifNull": ["$timezone", "Australia/Melbourne"]
            }
        }
    }]

    org_id_rows = exec_mongo_pipeline(
        spark, org_timezone_pipeline, 'Organization',
        org_timezone_schema).select("*").collect()

    df = None

    for org_id_row in org_id_rows:
        org_id = org_id_row.asDict()['org_id']

        data_upload_audit_pipeline = [{
            "$match": {
                "org_id": org_id
            }
        }, {
            "$project": {
                "user_id" : 1,
                "job_name" : 1, 
                "audit_file" : 1, 
                "start_date" : {
                    "$dateToString": {
                        "format": "%Y-%m-%dT%H:%M:%SZ",
                        "date": "$start_date"
                    }
                },
                "end_date" : {
                    "$dateToString": {
                        "format": "%Y-%m-%dT%H:%M:%SZ",
                        "date": "$end_date"
                    }
                }, 
                "runID" : 1, 
                "file_name" : 1, 
                "status" : 1, 
                "entity_name" : 1,
                "message":1,
                "record_inserted" : 1,
                "org_id": 1 
            }
        }]

        data_upload_logs = exec_mongo_pipeline(spark, data_upload_audit_pipeline, 'ETL_Audit_Log', schema)

        if df is None:
            df = data_upload_logs
        else:
            df = df.union(data_upload_logs)

    df.createOrReplaceTempView("data_upload_audit_log")


    df = spark.sql("""
                    select  distinct user_id.oid as userId,
                            job_name as jobName,
                            audit_file as auditFile,
                            runID as runID,
                            file_name as fileName,
                            status as status,
                            entity_name as entityName,
                            cast(start_date as date) as startDate,
                            cast(end_date as date) as endDate,
                            message as message,
                            record_inserted as recordInserted,
                            lower(org_id) as orgId
                    from data_upload_audit_log
                """)

    delta_table_partition_ovrewrite(
        df, "dg_performance_management.data_upload_audit_log", ['orgId'])
