from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from dganalytics.utils.utils import exec_mongo_pipeline, delta_table_partition_ovrewrite, get_active_organization_timezones
from datetime import datetime, timedelta
from bson import json_util
import json

schema = StructType([
    StructField('campaignId',StringType(), True),
    StructField('activityId', StringType(), True),
    StructField('userId', StringType(), True),
    StructField('points', IntegerType(), True),
    StructField('outcomeType', StringType(), True),
    StructField('teamId', StringType(), True),
    StructField('kpiName', StringType(), True),
    StructField('orgId', StringType(), True),
    StructField('fieldName', StringType(), True),
    StructField('fieldValue', DoubleType(), True),
    StructField('frequency', StringType(), True),
    StructField('entityName', StringType(), True),
    StructField('noOfTimesPerformed', IntegerType(), True),
    StructField('activityName', StringType(), True),
    StructField('target', DoubleType(), True),
    StructField('date', StringType(), True),
    StructField('mongoUserId', StringType(), True),
    StructField('awardedBy', StringType(), True)
])
def get_activity_wise_points(spark):
  Current_Date = datetime.now()
  extract_end_time = Current_Date.strftime('%Y-%m-%dT%H:%M:%S.%fZ') 
  extract_start_time = (Current_Date - timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
  for org_timezone in get_active_organization_timezones().rdd.collect():
    org_id = org_timezone['org_id']
    org_timezone = org_timezone['timezone']

    activity_points_pipeline = [
      {
        "$match": {
            "org_id": org_id,
            "outcome_type": "points",
            "$or":[
              {
                'creation_date': {
                  '$gte': { '$date': extract_start_time },
                  '$lte': { '$date': extract_end_time }
                 }
              },
              {
                'last_updated': {
                  '$gte': { '$date': extract_start_time },
                  '$lte': { '$date': extract_end_time }
                 }
              }
            ]
            
        }
      }, 
      {
        "$project": {
            "campaignId": "$campaign_id",
            "activityId": "$outcome_id",
            "userId" : "$user_id",
            "points": "$outcome_quantity",
            "outcomeType" : "$outcome_type",
            "teamId" : "$team_id",
            "kpiName" : "$kpi_name",
            "orgId" : "$org_id",
            "fieldName" : "$field_name",
            "fieldValue": "$field_value",
            "frequency": 1,
            "entityName": "$entity_name",
            "noOfTimesPerformed" : "$no_of_times_performed",
            "activityName": "$outcome_name",
            "target": "$quartile_target",
            "date": {
                "$cond": {
                    "if": {
                        "$and": [{
                            "$eq": ["$creation_date", None]
                        }]
                    },
                    "then": None,
                    "else": {
                        "$dateToString": {
                            "format": "%Y-%m-%d",
                            "date": "$creation_date",
                            "timezone": org_timezone
                          }
                      }
                }
           }
       }
      }  
    ]
    badge_bonus_points_pipeline = [
      {
        "$match": {
            "org_id": org_id,
            "outcome_type": "badge",
            'creation_date': {
                  '$gte': { '$date': extract_start_time },
                  '$lte': { '$date': extract_end_time }
                 }
        }
      }, { 
        "$project": {
            "outcome_quantity": 1,
            "user_id": 1,
            "campaign_id": 1,
            "awarded_by": 1,
            "activity_name": "Badges Bonus Points",
            "creation_date": 1,
            "outcome_type": 1,
            "org_id": 1
        }
      }, {
        "$lookup": {
            "from":
            "User",
            "let": {
                "userid": "$user_id"
            },
            "pipeline": [{
                "$match": {
                    "$expr": {
                        "$and": [{
                            "$eq": ["$user_id", "$$userid"]
                        }]
                    }
                }
            }, {
                "$project": {
                    "_id": 1.0,
                    "org_id": 1.0
                }
            }],
            "as":
            "users"
        }
      }, {
        "$unwind": {
            "path": "$users",
            "preserveNullAndEmptyArrays": False
        }
      }, {
        "$project": {
            "points": "$outcome_quantity",
            "userId": "$user_id",
            "campaignId": "$campaign_id",
            "awardedBy": "$awarded_by",
            "activityName": 1,
            "date": {
                "$dateToString": {
                    "format": "%Y-%m-%d",
                    "date": {
                        "$toDate": {
                            "$dateToString": {
                                "date": "$creation_date",
                                "timezone": org_timezone
                            }
                        }
                    }
                }
            },
            "mongoUserId": "$users._id",
            "orgId": "$org_id",
            "outcomeType": "$outcome_type"
        }
      }
    ]
  
    activity_points_df = exec_mongo_pipeline(spark,activity_points_pipeline,'User_Outcome', schema)
    badge_bonus_points_df = exec_mongo_pipeline(spark,badge_bonus_points_pipeline,'User_Outcome', schema)
    points_df = activity_points_df.union(badge_bonus_points_df)
    #points_df.display()

    points_df.createOrReplaceTempView("activity_wise_points")
    spark.sql("""
        MERGE INTO dg_performance_management.activity_wise_points AS target
        USING activity_wise_points AS source
        ON target.orgId = source.orgId
        AND target.userId = source.userId
        AND target.campainId = source.campainId
        AND target.activityId = source.activityId
        AND target.date= source.date
        WHEN NOT MATCHED THEN
         INSERT *        
      """)

