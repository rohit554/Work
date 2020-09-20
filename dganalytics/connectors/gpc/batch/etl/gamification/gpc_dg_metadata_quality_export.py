from dganalytics.utils.utils import get_spark_session, get_gamification_token, get_secret, push_gamification_data
from dganalytics.connectors.gpc.gpc_utils import dg_metadata_export_parser, get_dbname, gpc_utils_logger
import io
import requests
from pyspark.sql import SparkSession
import pandas as pd
import http.client
import mimetypes


def get_telephony_data(spark: SparkSession, extract_date: str, org_id: str):
    df = spark.sql(f"""
                select cast(a.releaseDate as date) as `Evaluation Date`, a.agentId as UserID, avg(b.totalScore) as `QA Score`
                    from dim_evaluations a, fact_evaluation_total_scores b
                where a.evaluationId  = b.evaluationId
                    and cast(a.releaseDate as date) >= cast('{extract_date}' as date)
                        group by cast(a.releaseDate as date), a.agentId 
                """)
    return df.toPandas()


if __name__ == "__main__":
    tenant, run_id, extract_date, org_id = dg_metadata_export_parser()
    db_name = get_dbname(tenant)
    app_name = "gpc_dg_metadata_Quality_export"
    spark = get_spark_session(app_name, tenant, default_db=db_name)
    logger = gpc_utils_logger(tenant, app_name)
    try:
        logger.info("gpc_dg_metadata_Quality_export")

        df = get_telephony_data(spark, extract_date, org_id)
        push_gamification_data(df, org_id, 'Quality')

    except Exception as e:
        logger.exception(e, stack_info=True, exc_info=True)
        raise
