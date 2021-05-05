from dganalytics.utils.utils import get_spark_session, push_gamification_data, export_powerbi_csv
from dganalytics.connectors.gpc.gpc_utils import dg_metadata_export_parser, get_dbname, gpc_utils_logger, get_path_vars
from pyspark.sql import SparkSession
import os
import pandas as pd

def get_probeabg_data(spark: SparkSession, extract_date: str):
    backword_days = 7

    df = spark.sql(f"""
                    SELECT  U.userId UserId,
                            U.date Date,
                            U.department,
                            FCM.AvgDailyHoldTimeVoice,
                            FCM.AvgDailyHoldTimeMessage,
                            FCM.AvgDailyHoldTimeEmail,
                            FCM.AvgDailyHoldTime,
                            FCM.AvgDailyAcwTimeVoice,
                            FCM.AvgDailyAcwTimeMessage,
                            FCM.AvgDailyAcwTimeEmail,
                            FCM.AvgDailyAcwTime,
                            FCM.AvgDailyHandleTimeVoice,
                            FCM.AvgDailyHandleTimeMessage,
                            FCM.AvgDailyHandleTimeEmail,
                            FCM.AvgDailyHandleTime,
                            WFM.DailyAdherencePercentage
                    FROM (SELECT   userId,
                                    date,
                                    department
                            FROM gpc_probeabg.dim_users,
                            (SELECT EXPLODE(SEQUENCE((CAST('{extract_date}' AS date))- {backword_days} + 2, (CAST('{extract_date}' as date))+1, interval 1 day )) as date) dates) AS U
                    FULL OUTER JOIN (
                        SELECT agentId,
                            CAST(from_utc_timestamp(emitDateTime, 'Australia/Sydney') AS date) AS Date,
                            SUM(CASE WHEN lower(mediaType) = 'voice' THEN COALESCE(tHeldComplete, 0) ELSE 0 END)/ SUM(CASE WHEN mediaType = 'voice' THEN COALESCE(nHeldComplete, 0) ELSE 0 END) AS AvgDailyHoldTimeVoice,
                            SUM(CASE WHEN lower(mediaType) = 'message' THEN COALESCE(tHeldComplete, 0) ELSE 0 END)/ SUM(CASE WHEN mediaType = 'message' THEN COALESCE(nHeldComplete, 0) ELSE 0 END) AS AvgDailyHoldTimeMessage,
                            SUM(CASE WHEN lower(mediaType) = 'email' THEN COALESCE(tHeldComplete, 0) ELSE 0 END)/ SUM(CASE WHEN mediaType = 'email' THEN COALESCE(nHeldComplete , 0) ELSE 0 END) AS AvgDailyHoldTimeEmail,
                            SUM(coalesce(tHeldComplete, 0))/ SUM(COALESCE(nHeldComplete, 0)) AS AvgDailyHoldTime,
                            SUM(CASE WHEN lower(mediaType) = 'voice' THEN COALESCE(tAcw, 0) ELSE 0 END)/ SUM(CASE WHEN mediaType = 'voice' THEN COALESCE(nAcw , 0) ELSE 0 END) AS AvgDailyAcwTimeVoice,
                            SUM(CASE WHEN lower(mediaType) = 'message' THEN COALESCE(tAcw, 0) ELSE 0 END)/ SUM(CASE WHEN mediaType = 'message' THEN COALESCE(nAcw , 0) ELSE 0 END) AS AvgDailyAcwTimeMessage,
                            SUM(CASE WHEN lower(mediaType) = 'email' THEN COALESCE(tAcw, 0) ELSE 0 END)/ SUM(CASE WHEN mediaType = 'email' THEN COALESCE(nAcw , 0) ELSE 0 END) AS AvgDailyAcwTimeEmail,
                            SUM(COALESCE(tAcw, 0))/ SUM(COALESCE(nAcw , 0)) as AvgDailyAcwTime,
                            SUM(CASE WHEN lower(mediaType) = 'voice' THEN COALESCE(thandle, 0) ELSE 0 END)/ SUM(CASE WHEN mediaType = 'voice' THEN COALESCE(nhandle , 0) ELSE 0 END) AS AvgDailyHandleTimeVoice,
                            SUM(CASE WHEN lower(mediaType) = 'message' THEN COALESCE(thandle, 0) ELSE 0 END)/ SUM(CASE WHEN mediaType = 'message' THEN COALESCE(nhandle , 0) ELSE 0 END) AS AvgDailyHandleTimeMessage,
                            SUM(CASE WHEN lower(mediaType) = 'email' THEN COALESCE(thandle, 0) ELSE 0 END)/ SUM(CASE WHEN mediaType = 'email' THEN COALESCE(nhandle , 0) ELSE 0 END) AS AvgDailyHandleTimeEmail,
                            SUM(COALESCE(thandle, 0))/ SUM(COALESCE(nhandle , 0)) as AvgDailyHandleTime
                        FROM gpc_probeabg.fact_conversation_metrics
                        WHERE CAST(from_utc_timestamp(emitDateTime, 'Australia/Sydney') AS date) <= (CAST('{extract_date}' AS date))
                          and CAST(from_utc_timestamp(emitDateTime, 'Australia/Sydney') AS date) >= (CAST('{extract_date}' AS date) - {backword_days})
                        GROUP BY agentId, CAST(from_utc_timestamp(emitDateTime, 'Australia/Sydney') AS date)) AS FCM
                      ON U.userId = FCM.agentId
                        AND U.date = FCM.Date
                    FULL OUTER JOIN (
                      SELECT genesysUserId as UserID, Date, TimeAdheringToSchedule as DailyAdherencePercentage
                      FROM dg_probeabg.wfm_verint_export
                      WHERE 
                       `Date` <= (CAST('{extract_date}' AS DATE))
                       AND `Date` >= (CAST('{extract_date}' AS DATE) - {backword_days})
                    ) WFM
                    ON U.userId = WFM.UserID
                        AND U.date = WFM.Date
                      WHERE U.department IS NOT NULL
	                        AND U.userId IS NOT NULL
                            AND U.Date IS NOT NULL
                """)

    df.cache()
    return df

def push_sales_data(spark):
    sales = spark.sql("""
        SELECT * FROM (
            SELECT 
                UserId AS `UserId`,
                date_format(cast(Date as date), 'dd/MM/yyyy') AS `Date`,
                AvgDailyAcwTime AS ACW,
                AvgDailyHoldTime AS `Hold time`,
                AvgDailyHandleTime AS AHT
                
            FROM
            probeabg_game_data
            WHERE department in ('Sales')
        )
        WHERE NOT (ACW IS NULL AND `Hold time` IS NULL AND AHT IS NULL)
    """)
    push_gamification_data(sales.toPandas(), 'PROBEABGSALES', 'ProbeABGSales')
    return True

def push_service_data(spark):
    service = spark.sql("""
        SELECT * FROM (
            SELECT 
                UserID `UserId`,
                date_format(cast(Date as date), 'dd/MM/yyyy') `Date`,
                AvgDailyAcwTime ACW,
                AvgDailyHoldTime `Average hold time`,
                AvgDailyHandleTime AHT,
                DailyAdherencePercentage Adherence
            FROM
            probeabg_game_data
            WHERE department in ('Service')
        )
        WHERE NOT (ACW IS NULL AND `Average hold time` IS NULL AND AHT IS NULL AND Adherence IS NULL)
    """)
    push_gamification_data(service.toPandas(), 'PROBEABGSERVICE', 'ProbeABGServices')
    return True

if __name__ == "__main__":
    tenant, run_id, extract_date, org_id = dg_metadata_export_parser()
    tenant = 'probeabg'
    db_name = get_dbname(tenant)
    app_name = "probeabg_push_gamification_data"
    spark = get_spark_session(app_name, tenant, default_db=db_name)
    logger = gpc_utils_logger(tenant, app_name)
    try:
        logger.info("probeabg_push_gamification_data")

        df = get_probeabg_data(spark, extract_date)
        df = df.drop_duplicates()
        df.registerTempTable("probeabg_game_data")

        push_sales_data(spark)
        push_service_data(spark)

    except Exception as e:
        logger.exception(e, stack_info=True, exc_info=True)
        raise
