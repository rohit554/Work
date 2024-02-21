from dganalytics.utils.utils import exec_mongo_pipeline, get_active_organization_timezones
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from datetime import datetime, timedelta
from pyspark.sql.functions import lower

def build_pipeline(org_id: str, org_timezone: str):  
    extract_start_time = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    pipeline = [
        {
            "$match": {
                "org_id": org_id,
                'start_date': {
                  '$gte': { '$date': extract_start_time }
                 }
            }
        }, 
        {
            "$project": {
                "orgId" : "$org_id",
                "challengerMongoId": "$challenger_user_id",
                "campaignId": "$campaign_id",
                "challengeName": "$challenge_name",
                "challengeFrequency": "$frequency",
                "noOfDays": "$no_of_days",
                "challengeeMongoId": "$challengee_user_id",
                "status": 1.0,
                "action": 1.0,
                "challengeEndDate": {
                    "$cond": {
                        "if": {
                            "$and": [
                                {
                                    "$eq": [
                                        "$end_date",
                                        None
                                    ]
                                }
                            ]
                        },
                        "then": None,
                        "else": {
                            "$dateToString": {
                                "format": "%Y-%m-%d",
                                "date": "$end_date",
                                "timezone": org_timezone  
                            }
                        }
                    }
                },
                "challengeThrownDate": {
                    "$dateToString": {
                        "format": "%Y-%m-%d",
                        "date": {
                            "$toDate": {
                                "$toLong": "$creation_date_str"
                            }
                          },
                        "timezone": org_timezone
                    }
                },
                "challengeAcceptanceDate": {
                    "$cond": {
                        "if": {
                            "$and": [
                                {
                                    "$eq": [
                                        "$start_date",
                                        None
                                    ]
                                }
                            ]
                        },
                        "then": None,
                        "else": {
                            "$dateToString": {
                                "format": "%Y-%m-%d",
                                "date": "$start_date",
                                "timezone": org_timezone
                              
                            }
                        }
                    }
                },
                "challengeCompletionDate": {
                    "$cond": {
                        "if": {
                            "$and": [
                                {
                                    "$eq": [
                                        "$completion_date",
                                        None
                                    ]
                                }
                            ]
                        },
                        "then": None,
                        "else": {
                            "$dateToString": {
                                "format": "%Y-%m-%d",
                                "date": "$completion_date",
                                "timezone": org_timezone
                                        
                                
                            }
                        }
                    }
                }
            }
        }
    ]
    return pipeline

schema = StructType([StructField('action', StringType(), True),
                     StructField('campaignId', StringType(), True),
                     StructField('challengeAcceptanceDate', StringType(), True),
                     StructField('challengeCompletionDate', StringType(), True),
                     StructField('challengeEndDate', StringType(), True),
                     StructField('challengeFrequency', IntegerType(), True),
                     StructField('challengeName', StringType(), True),
                     StructField('challengeThrownDate', StringType(), True),
                     StructField('challengeeMongoId', StringType(), True),
                     StructField('challengerMongoId', StringType(), True),
                     StructField('noOfDays', IntegerType(), True),
                     StructField('orgId', StringType(), True),
                     StructField('status', StringType(), True)])


def get_challenges(spark):
    # Get campaign & challenges for each org
    for org_timezone in get_active_organization_timezones(spark).rdd.collect():
        org_id = org_timezone['org_id']
        org_timezone = org_timezone['timezone']

        pipeline = build_pipeline(org_id, org_timezone)

        challenges_df = exec_mongo_pipeline(spark, pipeline, 'User_Challenges', schema)
        challenges_df = challenges_df.withColumn("orgId", lower(challenges_df["orgId"]))
        
        challenges_df.createOrReplaceTempView("challenges")
        
        spark.sql(f"""
                DELETE FROM dg_performance_management.challenges target
                WHERE target.orgId = lower('{org_id}')
                AND
                EXISTS (
                    SELECT 1
                    FROM challenges source
                    WHERE source.challengeeMongoId = target.challengeeMongoId
                    AND source.campaignId = target.campaignId
                    AND source.challengerMongoId = target.challengerMongoId
                    AND source.challengeThrownDate = target.challengeThrownDate
                    AND source.challengeName = target.challengeName
                    AND (source.challengeAcceptanceDate = target.challengeAcceptanceDate OR (source.challengeAcceptanceDate = target.challengeAcceptanceDate IS NULL AND target.challengeAcceptanceDate IS NULL) )
                )
         """)     
            

        spark.sql("""
            INSERT INTO dg_performance_management.challenges (action,campaignId,challengeThrownDate,challengeAcceptanceDate,challengeCompletionDate,challengeEndDate,challengeFrequency,challengeName,challengeeMongoId,challengerMongoId,noOfDays,orgId,status)
            SELECT
                action,
                campaignId,
                CAST(challengeThrownDate AS DATE) AS challengeThrownDate,
                CAST(challengeAcceptanceDate AS DATE) AS challengeAcceptanceDate,
                CAST(challengeCompletionDate AS DATE) AS challengeCompletionDate,
                challengeEndDate,
                challengeFrequency,
                challengeName,
                challengeeMongoId,
                challengerMongoId,
                noOfDays,
                orgId,
                status
            FROM challenges
        """)
      
