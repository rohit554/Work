from dganalytics.utils.utils import get_spark_session, push_gamification_data, export_powerbi_csv
from dganalytics.connectors.gpc.gpc_utils import dg_metadata_export_parser, get_dbname, gpc_utils_logger
from pyspark.sql import SparkSession


def get_coles_data(spark: SparkSession, extract_date: str, org_id: str):
    back_days = 7
    df = spark.sql(f"""
        WITH UserDates AS(
            SELECT u.userId,
                    date_format(D._date, 'dd-MM-yyyy') _date
            FROM gpc_salmatcolesonline.dim_users U
            CROSS JOIN (SELECT explode(sequence(CAST('{extract_date}' AS Date) - {back_days}, CAST('{extract_date}' AS Date), interval 1 day)) _date) as D
            WHERE U.state = 'active')

        SELECT * FROM (
        SELECT  UD.userId,
                UD.`_date` Date,
                SUM(tHeldComplete) tHeldComplete,
                SUM(nHandle) nHandle,
                SUM(tAcw) tAcw,
                COUNT(wrapUpCode) wrapUpCodeCount,
                SUM(FW.adherenceScheduleSecs) adherenceScheduleSecs,
                SUM(exceptionDurationSecs) exceptionDurationSecs
        FROM UserDates UD
        LEFT JOIN gpc_salmatcolesonline.fact_conversation_metrics FC
            ON  FC.agentId = UD.userId
                AND date_format(cast(from_utc_timestamp(emitDateTime, 'Australia/Sydney') as date), 'dd-MM-yyyy') = UD.`_date`
        LEFT JOIN gpc_salmatcolesonline.fact_wfm_day_metrics FW
            ON FW.userId = UD.userId
            AND date_format(cast(from_utc_timestamp(startDate, 'Australia/Sydney') as date), 'dd-MM-yyyy') = UD.`_date`
        GROUP BY UD.userId, UD.`_date`
        )
        WHERE NOT (
            tHeldComplete IS NULL
            AND nHandle IS NULL
            AND tAcw IS NULL
            AND (wrapUpCodeCount IS NULL OR wrapUpCodeCount = 0)
            AND (adherenceScheduleSecs IS NULL OR adherenceScheduleSecs = 0)
            AND (exceptionDurationSecs IS NULL OR exceptionDurationSecs = 0) 
        )
    """)
    return df


if __name__ == "__main__":
    tenant, run_id, extract_date, org_id = dg_metadata_export_parser()
    db_name = get_dbname(tenant)
    app_name = "gpc_dg_metadata_colesonline_export"
    spark = get_spark_session(app_name, tenant, default_db=db_name)
    logger = gpc_utils_logger(tenant, app_name)
    try:
        logger.info("gpc_dg_metadata_colesonline_export")

        df = get_coles_data(spark, extract_date, org_id)
        df = df.drop_duplicates()
        push_gamification_data(
            df.toPandas(), 'SALMATCOLESONLINE', 'ProbecolesConnection')

    except Exception as e:
        logger.exception(e, stack_info=True, exc_info=True)
        raise
