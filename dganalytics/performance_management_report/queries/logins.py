%python
from dganalytics.utils.utils import exec_mongo_pipeline, delta_table_partition_ovrewrite, get_path_vars
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DateType
from pyspark.sql.functions import col
import pandas as pd
import os
pipeline = [
        { 
            "$lookup" : { 
                "from" : "User", 
                "let" : { 
                    "user_id" : "$user_id"
                }, 
                "pipeline" : [
                    { 
                        "$match" : { 
                            "$expr" : { 
                                "$and" : [
                                    { 
                                        "$eq" : [
                                            "$user_id", 
                                            "$$user_id"
                                        ]
                                    }
                                ]
                            }
                        }
                    }, 
                    { 
                        "$project" : { 
                            "org_id" : 1.0
                        }
                    }
                ], 
                "as" : "users"
            }
        }, 
        { 
            "$unwind" : { 
                "path" : "$users", 
                "preserveNullAndEmptyArrays" : False
            }
        }, 
        { 
            "$project" : { 
                "_id" : 0.0, 
                "user_id" : 1.0, 
                "login_attempt" : 1.0, 
                "date" : "$timestamp", 
                "org_id" : "$users.org_id"
            }
        }, 
        { 
            "$lookup" : { 
                "from" : "Organization", 
                "let" : { 
                    "oid" : "$org_id"
                }, 
                "pipeline" : [
                    { 
                        "$match" : { 
                            "$expr" : { 
                                "$and" : [
                                    { 
                                        "$eq" : [
                                            "$org_id", 
                                            "$$oid"
                                        ]
                                    }, 
                                    { 
                                        "$eq" : [
                                            "$type", 
                                            "Organisation"
                                        ]
                                    }
                                ]
                            }
                        }
                    }, 
                    { 
                        "$project" : { 
                            "timezone" : { 
                                "$ifNull" : [
                                    "$timezone", 
                                    "Australia/Melbourne"
                                ]
                            }
                        }
                    }
                ], 
                "as" : "org"
            }
        }, 
        { 
            "$unwind" : { 
                "path" : "$org", 
                "preserveNullAndEmptyArrays" : False
            }
        }, 
        { 
            "$project" : { 
                "_id" : 0.0, 
                "user_id" : 1.0, 
                "login_attempt" : 1.0, 
                "date" : { 
                    "$dateToString" : { 
                        "format" : "%Y-%m-%dT%H:%M:%SZ", 
                        "date" : { 
                            "$toDate" : { 
                                "$dateToString" : { 
                                    "date" : "$date", 
                                    "timezone" : "$org.timezone"
                                }
                            }
                        }
                    }
                }, 
                "org_id" : 1.0
            }
        }
    ]

schema = StructType([StructField('date', StringType(), True),
                    StructField('org_id', StringType(), True),
                     StructField('login_attempt', IntegerType(), True),
                     StructField('user_id', StringType(), True)])
def get_hf_timezones(spark):
  tenant_path, db_path, log_path = get_path_vars("hellofresh")  
  user_timezone = pd.read_csv(os.path.join(tenant_path, 'data', 'config', 'User_Group_region_Sites.csv'), header=0)
  user_timezone = spark.createDataFrame(user_timezone)
  return user_timezone
def get_genesys_clients_attendance(spark,tenant):  
  if tenant == 'hellofreshanz' :
    user_timezone = get_hf_timezones(spark)
    user_timezone.createOrReplaceTempView("user_timezone")
    attendance_df = spark.sql(f"""
                              SELECT 
                              fp.userId, 
                              from_utc_timestamp(lag(fp.endTime, 1) OVER (PARTITION BY fp.userId ORDER BY fp.startTime),trim(ut.timeZone)) tZStartTime,
                              CASE WHEN tZStartTime is NULL AND CAST(((unix_timestamp(fp.endTime) - unix_timestamp(fp.startTime))/3600) AS INT) > 12 THEN 
                                  from_utc_timestamp(fp.startTime-INTERVAL 12 HOURS,trim(ut.timeZone)) 
                              ELSE from_utc_timestamp(lag(fp.endTime, 1) OVER (PARTITION BY fp.userId ORDER BY fp.startTime),trim(ut.timeZone))
                              END actualStartTime,
                              from_utc_timestamp(fp.startTime,trim(ut.timeZone)) as actualEndTime
                              FROM 
                                  gpc_hellofresh.fact_user_presence fp, user_timezone ut
                              JOIN (select * from dg_performance_management.users where orgId="hellofreshanz") pmu
                              ON fp.userId=pmu.userId
                              WHERE 
                                  fp.userId = ut.userId
                                  AND fp.systemPresence IN ('OFFLINE')
                                  AND CAST(((unix_timestamp(fp.endTime) - unix_timestamp(fp.startTime))/3600) AS INT) > 4
                              """)
    return attendance_df
  else:
    tenant_timezones = {"salmatcolesonline":"Australia/Melbourne","skynzib":"Pacific/Auckland","skynzob":"Pacific/Auckland"}
    dbname = tenant
    if tenant == 'skynzib' or tenant == 'skynzob':
      dbname="skynz"
    attendance_df = spark.sql(f"""
                              SELECT 
                              fp.userId, 
                              from_utc_timestamp(lag(fp.endTime, 1) OVER (PARTITION BY fp.userId ORDER BY fp.startTime),"{tenant_timezones[tenant]}") tZStartTime,
                              CASE WHEN tZStartTime is NULL AND CAST(((unix_timestamp(fp.endTime) - unix_timestamp(fp.startTime))/3600) AS INT) > 12 THEN 
                                  from_utc_timestamp(fp.startTime-INTERVAL 12 HOURS,"{tenant_timezones[tenant]}") 
                              ELSE from_utc_timestamp(lag(fp.endTime, 1) OVER (PARTITION BY fp.userId ORDER BY fp.startTime),"{tenant_timezones[tenant]}")
                              END actualStartTime,
                              from_utc_timestamp(fp.startTime,"{tenant_timezones[tenant]}") as actualEndTime
                              FROM 
                                  gpc_{dbname}.fact_primary_presence fp
                              JOIN (select * from dg_performance_management.users where orgId = "{tenant}") pmu
                              ON fp.userId=pmu.userId
                              WHERE 
                                  fp.systemPresence IN ('OFFLINE')
                                  AND CAST(((unix_timestamp(fp.endTime) - unix_timestamp(fp.startTime))/3600) AS INT) > 4
                              
                              """)
    #attendance_df.display()
    return attendance_df


def get_logins(spark):
  df = exec_mongo_pipeline(spark, pipeline, 'Audit_Log', schema)
  df.createOrReplaceTempView("logins")
  genesys_tenants = ['hellofreshanz','salmatcolesonline','skynzib','skynzob']
  genesys_attendance_schema = StructType([
    StructField("userId", StringType(), True),
    StructField("tZStartTime", TimestampType(), True),
    StructField("actualStartTime", TimestampType(), True),
    StructField("actualEndTime", TimestampType(), True)
])
  genesys_logins = spark.createDataFrame([], schema=genesys_attendance_schema)
  
  for tenant in genesys_tenants:
    genesys_logins = genesys_logins.union(get_genesys_clients_attendance(spark,tenant))
   
  genesys_logins.createOrReplaceTempView("genesys_attendance")

  user_timezone = get_hf_timezones(spark)
  user_timezone.createOrReplaceTempView("user_timezone")

  hf_df=df.filter(col("org_id") == "HELLOFRESHANZ")
  hf_df = hf_df.withColumnRenamed("user_id", "userId")
  hf_df = hf_df.join(user_timezone, on="userId", how="left")
  hf_df.createOrReplaceTempView("hf_logins")

  df = spark.sql("""
                    select  distinct cast(date as date) date,
                            login_attempt loginAttempt,
                            user_id userId,
                            lower(org_id) orgId
                    from logins
                    WHERE org_id NOT IN ('TPINDIAIT','HELLOFRESHANZ','SALMATCOLESONLINE','SKYNZIB','SKYNZOB')
                    UNION ALL
                    SELECT DISTINCT (case when A.reportDate IS NULL then cast(L.date as date) else
                            to_date(A.reportDate, 'dd-MM-yyyy') end) as date,
                            login_attempt AS loginAttempt,
                            user_id AS userId,
                            LOWER(org_id) AS orgId
                    FROM logins L
                    LEFT JOIN dg_performance_management.attendance A
                        on L.user_id = A.userId
                        and lower(L.org_id) = A.orgId
                        AND CAST(L.date AS TIMESTAMP) BETWEEN A.loginTime and A.logoutTime
                    WHERE org_id IN ('TPINDIAIT')
                    UNION ALL
                    SELECT DISTINCT (case when A.actualStartTime IS NULL then cast(L.date as date) else
                            to_date(A.actualStartTime, 'dd-MM-yyyy') end) as date,
                            login_attempt AS loginAttempt,
                            user_id AS userId,
                            LOWER(org_id) AS orgId
                    FROM logins L
                    LEFT JOIN genesys_attendance A
                        on L.user_id = A.userId
                        AND CAST(L.date AS TIMESTAMP) BETWEEN A.actualStartTime and A.actualEndTime
                    WHERE org_id IN ('SALMATCOLESONLINE','SKYNZIB','SKYNZOB')
                    UNION ALL
                    SELECT DISTINCT (case when A.actualStartTime IS NULL then cast(from_utc_timestamp(to_utc_timestamp(L.date, "Asia/Manila"),trim(timeZone)) as date) else
                            to_date(A.actualStartTime, 'dd-MM-yyyy') end) as date,
                            login_attempt AS loginAttempt,
                            L.userId AS userId,
                            LOWER(org_id) AS orgId
                    FROM hf_logins L
                    LEFT JOIN genesys_attendance A
                        on L.userId = A.userId
                        AND CAST(from_utc_timestamp(to_utc_timestamp(L.date, "Asia/Manila"),trim(timeZone)) AS TIMESTAMP) BETWEEN A.actualStartTime and A.actualEndTime
                    WHERE org_id IN ('HELLOFRESHANZ')
                """)
    
  df = df.withColumn("date", col("date").cast(DateType()))
  
    
  delta_table_partition_ovrewrite(df, "dg_performance_management.logins", ['orgId'])