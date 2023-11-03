from pyspark.sql.types import StructType, StructField, StringType, BooleanType
from dganalytics.utils.utils import exec_mongo_pipeline, get_active_organization_timezones
from datetime import datetime, timedelta
from pyspark.sql.functions import lower

schema = StructType([
                     StructField('is_deleted', BooleanType(), True),
                     StructField('is_active', BooleanType(), True),
                     StructField('creation_date',  StringType(), True),
                     StructField('priority', StringType(), True),
                     StructField('title', StringType(), True),
                     StructField('description', StringType(), True),
                     StructField('Announcement_sent_By', StringType(), True),
                     StructField('Announcement_received_By', StringType(), True),
                     StructField('Campaign_Name', StringType(), True),
                     StructField('orgId', StringType(), True),
                     StructField('campaign_id', StringType(), True)])

def get_announcement(spark):
    extract_start_time = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    for org_timezone in get_active_organization_timezones(spark).rdd.collect():
      org_id = org_timezone['org_id']
      org_timezone = org_timezone['timezone']
      pipeline = [
            {
                "$match" : {
                    'org_id' : org_id,
                    "creation_date":{
                        '$gte': { '$date': extract_start_time }
                    }
                }
            },
            {
                    '$lookup': {
                        'from': 'User', 
                        'let': {
                            'oid': '$sender'
                        }, 
                        'pipeline': [
                            {
                                '$match': {
                                    '$expr': {
                                        '$and': [
                                            {
                                                '$eq': [
                                                    '$_id', '$$oid'
                                                ]
                                            }
                                        ]
                                    }
                                }
                            }, {
                                '$project': {
                                    '_id': 0, 
                                    'user_id': 1
                                }
                            }
                        ], 
                        'as': 'sender_userId'
                    }
                }, 
                {
                    '$unwind': {
                        'path': '$sender_userId'
                    }
                },
                {
                    '$lookup': {
                        'from': 'Campaign', 
                        'let': {
                            'oid': '$campaign_id'
                        }, 
                        'pipeline': [
                            {
                                '$match': {
                                    '$expr': {
                                        '$and': [
                                            {
                                                '$eq': [
                                                    '$_id', '$$oid'
                                                ]
                                            }
                                        ]
                                    }
                                }
                            }, {
                                '$project': {
                                    '_id': 0, 
                                    'name': 1
                                }
                            }
                        ], 
                        'as': 'Campaign_Name'
                    }
                }, 
                {
                    '$unwind': {
                        'path': '$Campaign_Name', 
                        'preserveNullAndEmptyArrays': True
                    }
                }, 
                {
                    '$unwind': {
                        'path': '$recipient', 
                        'preserveNullAndEmptyArrays': True
                    }
                }, 
                {
                    '$lookup': {
                        'from': 'User', 
                        'let': {
                            'oid': '$recipient'
                        }, 
                        'pipeline': [
                            {
                                '$match': {
                                    '$expr': {
                                        '$and': [
                                            {
                                                '$eq': [
                                                    '$_id', '$$oid'
                                                ]
                                            }
                                        ]
                                    }
                                }
                            }, {
                                '$project': {
                                    '_id': 0, 
                                    'user_id': 1
                                }
                            }
                        ], 
                        'as': 'receiver_userId'
                    }
                }, 
                {
                    '$unwind': {
                        'path': '$receiver_userId', 
                        'preserveNullAndEmptyArrays': True
                    }
                }, 
                {
                    '$project': {
                        'is_deleted': 1, 
                        'is_active': 1, 
                        'orgId' : '$org_id', 
                        'creation_date': {
                            '$dateToString': {
                                'format': '%Y-%m-%dT%H:%M:%SZ', 
                                'date': '$creation_date', 
                                'timezone': '$org.timezone'
                            }
                        },
                        'priority': 1, 
                        'title': 1, 
                        'description': 1, 
                        'Announcement_sent_By': '$sender_userId.user_id', 
                        'Announcement_received_By': '$receiver_userId.user_id', 
                        'audience': 1, 
                        'campaign_id': 1, 
                        'Campaign_Name': '$Campaign_Name.name'
                    }
                }
            ]

      df = exec_mongo_pipeline(spark, pipeline, 'Announcement', schema)
      df = df.withColumn("orgId", lower(df["orgId"]))
      
      df.createOrReplaceTempView("announcement")
      spark.sql("""
        MERGE INTO dg_performance_management.announcement AS target
        USING announcement AS source
        ON target.orgId = source.orgId
        AND target.Announcement_sent_By = source.Announcement_sent_By
        AND target.Announcement_received_By = source.Announcement_received_By
        AND target.creation_date = source.creation_date
        WHEN MATCHED THEN
                UPDATE SET *
        WHEN NOT MATCHED THEN
         INSERT *        
      """)
      