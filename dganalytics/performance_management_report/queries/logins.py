
from dganalytics.utils.utils import exec_mongo_pipeline, delta_table_partition_ovrewrite, get_path_vars, get_active_organization_timezones
from pyspark.sql import SparkSession,Row
from pyspark.sql.functions import col, to_timestamp, lower
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from datetime import datetime, timedelta
import pandas as pd
import os

def get_hf_timezones(spark):
  tenant_path, db_path, log_path = get_path_vars("hellofresh")  
  user_timezone = pd.read_csv(os.path.join(tenant_path, 'data', 'config', 'User_Group_region_Sites.csv'), header=0)
  user_timezone = spark.createDataFrame(user_timezone)
  return user_timezone


def get_genesys_clients_attendance(spark,tenant,extractStartTime):  
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
                                  gpc_hellofresh.fact_primary_presence fp, user_timezone ut
                              JOIN (select * from dg_performance_management.users where orgId="hellofreshanz") pmu
                              ON fp.userId=pmu.userId
                              WHERE 
                                  fp.userId = ut.userId
                                  AND fp.systemPresence IN ('OFFLINE')
                                  AND CAST(((unix_timestamp(fp.endTime) - unix_timestamp(fp.startTime))/3600) AS INT) > 4
                                  AND from_utc_timestamp(startTime,trim(timeZone)) >= TIMESTAMP('{extractStartTime}')
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
                                  AND from_utc_timestamp(startTime,"{tenant_timezones[tenant]}") >= TIMESTAMP('{extractStartTime}')
                              """)
    return attendance_df


def get_logins(spark):
  extract_start_time = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
  logins_schema = StructType([
      StructField('date', StringType(), True),
      StructField('org_id', StringType(), True),
      StructField('login_attempt', IntegerType(), True),
      StructField('user_id', StringType(), True)
  ])

  for tenant in get_active_organization_timezones(spark).rdd.collect():
      logins_pipeline = [
          {
              '$match': {
                  'org_id': tenant['org_id'],
                  'timestamp': {
                      "$gte": {"$date":extract_start_time}
                  }
              }  
          },
          {
              '$project': {
                  '_id': 0,
                  'user_id': 1,
                  'login_attempt': 1,
                  "date": {
                      "$dateToString" : { 
                        "format" : "%Y-%m-%dT%H:%M:%SZ", 
                        "date" : "$timestamp", 
                        "timezone" : tenant['timezone']
                      }
                  },
                  'org_id': 1
              }
          }
      ]
      login_df = exec_mongo_pipeline(spark, logins_pipeline, 'Audit_Log', logins_schema)
   
      login_df=login_df.drop_duplicates()
      
      login_df.createOrReplaceTempView("logins")

      genesys_tenants = ['hellofreshanz','salmatcolesonline','skynzib','skynzob']
      genesys_attendance_schema = StructType([
        StructField("userId", StringType(), True),
        StructField("tZStartTime", TimestampType(), True),
        StructField("actualStartTime", TimestampType(), True),
        StructField("actualEndTime", TimestampType(), True)
      ])
      genesys_logins = spark.createDataFrame([], schema=genesys_attendance_schema)
      
      if tenant['org_id'] in genesys_tenants:
        genesys_logins = genesys_logins.union(get_genesys_clients_attendance(spark,tenant['org_id'], extract_start_time))
      
      genesys_logins.createOrReplaceTempView("genesys_attendance")

      if tenant['org_id'] == 'HELLOFRESHANZ':
        user_timezone = get_hf_timezones(spark)
        user_timezone.createOrReplaceTempView("user_timezone")

        hf_df=login_df.filter(col("org_id") == "HELLOFRESHANZ")
        hf_df = hf_df.withColumnRenamed("user_id", "userId")
        hf_df = hf_df.join(user_timezone, on="userId", how="left")
        hf_df.createOrReplaceTempView("hf_logins")
        updated_logins_df = spark.sql("""
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
      else:
        updated_logins_df = spark.sql("""
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

                                      """)
      
      updated_logins_df.createOrReplaceTempView("updated_logins")
      
      spark.sql("""
        MERGE INTO dg_performance_management.logins AS target
        USING updated_logins AS source
        ON target.orgId = source.orgId
        AND target.userId = source.userId
        AND target.date = source.date
        WHEN NOT MATCHED THEN
         INSERT *        
      """)
      