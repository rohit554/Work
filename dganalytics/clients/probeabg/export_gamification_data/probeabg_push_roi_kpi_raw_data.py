from dganalytics.utils.utils import get_spark_session, push_gamification_data, export_powerbi_csv
from dganalytics.connectors.gpc.gpc_utils import dg_metadata_export_parser, get_dbname, gpc_utils_logger
from pyspark.sql import SparkSession


def get_probeabg_data(spark, extract_date, logger):
    backword_days = 7
    try:
        df = spark.sql(f"""
						SELECT	U.userId,
								U.date,
								WFM.DailyAdherencePercentage,
								FCM.SumDailyHoldTimeVoice,
								FCM.CountDailyHoldTimeVoice,
								FCM.SumDailyHoldTimeMessage,
								FCM.CountDailyHoldTimeMessage,
								FCM.SumDailyHoldTimeEmail,
								FCM.CountDailyHoldTimeEmail,
								FCM.SumDailyHoldTime,
								FCM.CountDailyHoldTime,
								FCM.SumDailyAcwTimeVoice,
								FCM.CountDailyAcwTimeVoice,
								FCM.SumDailyAcwTimeMessage,
								FCM.CountDailyAcwTimeMessage,
								FCM.SumDailyAcwTimeEmail,
								FCM.CountDailyAcwTimeEmail,
								FCM.SumDailyAcwTime,
								FCM.CountDailyAcwTime,
								FCM.SumDailyHandleTimeVoice,
								FCM.CountDailyHandleVoice,
								FCM.SumDailyHandleTimeMessage,
								FCM.CountDailyHandleMessage,
								FCM.SumDailyHandleTimeEmail,
								FCM.CountDailyHandleEmail,
								FCM.SumDailyHandleTime,
								FCM.CountDailyHandle
								FROM (SELECT   userId,
												date,
												department
										FROM gpc_probeabg.dim_users,
										(SELECT EXPLODE(SEQUENCE((CAST('{extract_date}' AS date))- {backword_days} + 2, (CAST('{extract_date}' as date))+1, interval 1 day )) as date) dates) AS U
								LEFT JOIN (
									SELECT agentId,
											CAST(from_utc_timestamp(emitDateTime, 'Australia/Sydney') AS date) AS date, 
											SUM(CASE WHEN LOWER(mediaType) = 'voice' THEN COALESCE(tHeldComplete, 0) ELSE 0 END) AS SumDailyHoldTimeVoice,
											SUM(CASE WHEN LOWER(mediaType) = 'voice' THEN COALESCE(nHeldComplete, 0) ELSE 0 END) AS CountDailyHoldTimeVoice, 
											SUM(CASE WHEN LOWER(mediaType) = 'message' THEN COALESCE(tHeldComplete, 0) ELSE 0 END) AS SumDailyHoldTimeMessage,
											SUM(CASE WHEN LOWER(mediaType) = 'message' THEN COALESCE(nHeldComplete, 0) ELSE 0 END) AS CountDailyHoldTimeMessage, 
											SUM(CASE WHEN LOWER(mediaType) = 'email' THEN COALESCE(tHeldComplete, 0) ELSE 0 END) AS SumDailyHoldTimeEmail,
											SUM(CASE WHEN LOWER(mediaType) = 'email' THEN COALESCE(nHeldComplete , 0) ELSE 0 END) AS CountDailyHoldTimeEmail, 
											SUM(COALESCE(tHeldComplete, 0)) AS SumDailyHoldTime, 
											SUM(COALESCE(nHeldComplete, 0)) AS CountDailyHoldTime, 
											SUM(CASE WHEN LOWER(mediaType) = 'voice' THEN COALESCE(tAcw, 0) ELSE 0 END) AS SumDailyAcwTimeVoice, 
											SUM(CASE WHEN LOWER(mediaType) = 'voice' THEN COALESCE(nAcw , 0) ELSE 0 END) AS CountDailyAcwTimeVoice, 
											SUM(CASE WHEN LOWER(mediaType) = 'message' THEN coalesce(tAcw, 0) ELSE 0 END)  AS SumDailyAcwTimeMessage, 
											SUM(CASE WHEN LOWER(mediaType) = 'message' THEN coalesce(nAcw , 0) ELSE 0 END) AS CountDailyAcwTimeMessage, 
											SUM(CASE WHEN LOWER(mediaType) = 'email' THEN coalesce(tAcw, 0) ELSE 0 END) AS SumDailyAcwTimeEmail, 
											SUM(CASE WHEN LOWER(mediaType) = 'email' THEN coalesce(nAcw , 0) ELSE 0 END) AS CountDailyAcwTimeEmail, 
											SUM(COALESCE(tAcw, 0)) AS SumDailyAcwTime,
											SUM(COALESCE(nAcw , 0)) AS CountDailyAcwTime,
											SUM(CASE WHEN LOWER(mediaType) = 'voice' THEN COALESCE(tHandle , 0) ELSE 0 END) AS SumDailyHandleTimeVoice, 
											SUM(CASE WHEN LOWER(mediaType) = 'voice' THEN COALESCE(nHandle , 0) ELSE 0 END) AS CountDailyHandleVoice, 
											SUM(CASE WHEN LOWER(mediaType) = 'message' THEN COALESCE(tHandle , 0) else 0 end) as SumDailyHandleTimeMessage, 
											SUM(CASE WHEN LOWER(mediaType) = 'message' THEN COALESCE(nHandle , 0) else 0 end) as CountDailyHandleMessage, 
											SUM(CASE WHEN LOWER(mediaType) = 'email' THEN COALESCE(tHandle , 0) ELSE 0 END) AS SumDailyHandleTimeEmail, 
											SUM(CASE WHEN LOWER(mediaType) = 'email' THEN COALESCE(nHandle , 0) ELSE 0 END) AS CountDailyHandleEmail, 
											SUM(COALESCE(tHandle , 0)) AS SumDailyHandleTime,
											SUM(COALESCE(nHandle , 0)) AS CountDailyHandle
									FROM gpc_probeabg.fact_conversation_metrics 
									WHERE	CAST(from_utc_timestamp(emitDateTime, 'Australia/Sydney') AS date) <= (CAST('{extract_date}' AS date))
											AND CAST(from_utc_timestamp(emitDateTime, 'Australia/Sydney') AS date) >= (CAST('{extract_date}' AS date) - 7)
									GROUP BY	agentId,
												CAST(from_utc_timestamp(emitDateTime, 'Australia/Sydney') AS date)
								) FCM
								  ON	U.userId = FCM.agentId
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
								WHERE
									U.userId is not NULL and U.date is not NULL
					""")
    except Exception as e:
        logger.exception(e, stack_info=True, exc_info=True)
        raise
    return df


if __name__ == "__main__":
    tenant, run_id, extract_date, org_id = dg_metadata_export_parser()
    db_name = get_dbname(tenant)
    app_name = "probeabg_push_gamification_data"
    spark = get_spark_session(app_name, tenant, default_db=db_name)
    logger = gpc_utils_logger(tenant, app_name)
    try:
        logger.info("probeabg_push_gamification_data")

        df = get_probeabg_data(spark, extract_date, logger)
        df = df.drop_duplicates()
        df.createOrReplaceTempView("probeabg_activity")

        spark.sql(f"""
						MERGE INTO dg_probeabg.kpi_raw_data
						USING probeabg_activity 
							ON	kpi_raw_data.userId = probeabg_activity.userId
								AND kpi_raw_data.date = probeabg_activity.date
						WHEN MATCHED THEN 
							UPDATE SET *
						WHEN NOT MATCHED THEN
							INSERT *
					""")

        pb_export = spark.sql(
            "SELECT * FROM dg_probeabg.kpi_raw_data")
        export_powerbi_csv(tenant, pb_export, 'kpi_raw_data')

    except Exception as e:
        logger.exception(e, stack_info=True, exc_info=True)
        raise
