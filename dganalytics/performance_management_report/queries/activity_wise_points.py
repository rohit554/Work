from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from dganalytics.utils.utils import exec_mongo_pipeline, get_active_organization_timezones
from datetime import datetime, timedelta
from pyspark.sql.functions import lower

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
  extract_start_time = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
  for org_timezone in get_active_organization_timezones(spark).rdd.collect():
    org_id = org_timezone['org_id']
    org_timezone = org_timezone['timezone']

    activity_points_pipeline = [
      {
        "$match": {
            "org_id": org_id,
            "outcome_type": "points",
            '$expr': {
                '$or': [
                    {
                        '$gte': [
                            '$creation_date', { '$date': extract_start_time }
                        ]
                    }, {
                        '$gte': [
                            '$last_updated', { '$date': extract_start_time }
                        ]
                    }
                ]
            }
            
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
                "$dateToString": {
                    "format": "%Y-%m-%d",
                    "date": "$creation_date",
                    "timezone": org_timezone
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
                  '$gte': { '$date': extract_start_time }
                 }
        }
      }, { 
        "$project": {
            "outcome_quantity": 1,
            "user_id": 1,
            "campaign_id": 1,
            "awarded_by": 1,
            "activityName": "Badges Bonus Points",
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
  
    points_df = exec_mongo_pipeline(spark,activity_points_pipeline,'User_Outcome', schema)
    badge_bonus_points_df = exec_mongo_pipeline(spark,badge_bonus_points_pipeline,'User_Outcome', schema)
    points_df = points_df.union(badge_bonus_points_df)
    points_df = points_df.withColumn("orgId", lower(points_df["orgId"]))
    
    points_df.createOrReplaceTempView("activity_wise_points")
    spark.sql("""
                DELETE FROM dg_performance_management.activity_wise_points a
                WHERE EXISTS (
                    SELECT 1
                    FROM activity_wise_points source
                    WHERE source.orgId = a.orgId
                    AND source.userId = a.userId
                    AND source.campaignId = a.campaignId
                    AND cast(source.date as date) = a.date
                    AND source.outcomeType = a.outcomeType
                    AND (source.activityId = a.activityId OR (a.activityId IS NULL AND source.activityId IS NULL))        
                   
                )
         """)     
    spark.sql("""
                INSERT INTO dg_performance_management.activity_wise_points
                SELECT campaignId,
                       activityId,
                       userId,
                       points,
                       outcomeType,
                       teamId,
                       kpiName,
                       fieldName,
                       fieldValue,
                       frequency,
                       entityName,
                       noOfTimesPerformed,
                       activityName,
                       target,
                       cast(date as date) as date,
                       mongoUserId,
                       awardedBy,
                       orgId FROM activity_wise_points
      """)

