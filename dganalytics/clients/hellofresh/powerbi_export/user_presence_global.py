from dganalytics.utils.utils import get_spark_session, export_powerbi_csv, get_path_vars
from pyspark.sql import SparkSession
from dganalytics.connectors.gpc.gpc_utils import pb_export_parser, get_dbname, gpc_utils_logger
import os
import pandas as pd

def export_user_presence_global(spark: SparkSession, tenant: str, region: str):
    tenant_path, db_path, log_path = get_path_vars(tenant)
    user_timezones = pd.read_json(os.path.join(tenant_path, 'data', 'config', 'DG_Agent_Group_Site_Timezone.json'))
    user_timezones = pd.DataFrame(user_timezones['values'].tolist())
    header = user_timezones.iloc[0]
    user_timezones = user_timezones[1:]
    user_timezones.columns = header
    user_timezones = spark.createDataFrame(user_timezones)
    user_timezones.createOrReplaceTempView("user_timezones")
    user_presence_global = spark.sql(f"""
                          SELECT distinct userId,  
                                CAST(start_Time as Date) as startDate,
                                CONCAT(LPAD(HOUR(start_Time), 2, '0'), ':00:00 - ', LPAD(HOUR(start_Time) + 1, 2, '0'), ':00:00') AS TimeIntervalStart,
                                systemPresence,
                                TIMESTAMPDIFF(SECOND, start_Time, end_Time) AS timeDiff 
                                from  
                        (SELECT k.userId,from_utc_timestamp(start_Time, trim(ut.timeZone)) AS start_Time,
                        from_utc_timestamp(end_Time, trim(ut.timeZone)) as end_Time,systemPresence FROM(SELECT 
                            i.userId,  
                            
                                CASE  
                                    WHEN i.presenceInterval = i.start THEN i.startTime 
                                    ELSE i.presenceInterval  
                                END
                            AS start_Time,
                            
                                CASE  
                                    WHEN (i.presenceInterval + INTERVAL 1 HOUR) > i.endTime 
                                    THEN i.endTime 
                                    ELSE (i.presenceInterval + INTERVAL 1 HOUR) 
                                END AS end_Time,
                            i.systemPresence
                        FROM 
                            (
                                SELECT 
                                    userId, 
                                    systemPresence,
                                    startTime,
                                    endTime,
                                    DATE_TRUNC('hour', CAST(startTime AS TIMESTAMP)) AS start, 
                                    DATE_TRUNC('hour', CAST(endTime AS TIMESTAMP)) AS end,
                                    EXPLODE(SEQUENCE(
                                        DATE_TRUNC('hour', CAST(startTime AS TIMESTAMP)), 
                                        DATE_TRUNC('hour', CAST(endTime AS TIMESTAMP)), 
                                        INTERVAL 1 HOUR
                                    )) AS presenceInterval
                                FROM 
                                    gpc_hellofresh.fact_primary_presence
                                WHERE CAST(startTime AS DATE) >= add_months(CURRENT_DATE(), -5)
                        and systemPresence = "ON_QUEUE"
                                    AND startTime < endTime
                            ) i ) k
                        JOIN gpc_hellofresh.dim_user_groups u ON k.userId = u.userId
                        JOIN user_timezones ut ON u.groupName = ut.agentGroupName)                         
                                """)
    return user_presence_global