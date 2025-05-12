from dganalytics.utils.utils import get_spark_session, get_path_vars, push_gamification_data
from dganalytics.connectors.gpc.gpc_utils import (
    gpc_utils_logger,
    get_path_vars,
)
import concurrent.futures
import pandas as pd
import os
import argparse
import datetime

def get_config_csv_file_data(tenant_path, file_name):
    path = os.path.join(tenant_path, "data", "config", file_name)
    data = pd.read_csv(path)
    return data

def push_sales(spark, extract_start_time, extract_end_time):
    tenant_path, db_path, log_path = get_path_vars("hellofresh")

    user_df = get_config_csv_file_data(tenant_path, "User_Group_region_Sites.csv")
    user_df = spark.createDataFrame(user_df)
    user_df.createOrReplaceTempView("user_timezones")
    WRAPUP_CODES = [
        'Z-OUT-Success-1Week',
        'Z-OUT-Success-2Week',
        'Z-OUT-Success-3Week',
        'Z-OUT-Success-4Week',
        'Z-OUT-Success-5+Week-TL APPROVED'
    ]
    
    df = spark.sql(
        f"""
        SELECT 
            a.userId,
            date_format(from_utc_timestamp(a.conversationStart, d.timezone), 'yyyy/MM/dd HH:mm:ss') as date,
            1 as Sales
        FROM gpc_hellofresh.dim_last_handled_conversation a
        JOIN gpc_hellofresh.dim_wrapup_codes b ON a.wrapUpCodeId = b.wrapUpId
        JOIN gpc_hellofresh.dim_users c ON a.userId = c.userId
        JOIN user_timezones d ON a.userId = d.userId
        WHERE c.state = 'active'
        and a.conversationStart BETWEEN '{extract_start_time}' and '{extract_end_time}'
        AND b.wrapupCode IN ({', '.join([f"'{code}'" for code in WRAPUP_CODES])})
        """
    )
    push_gamification_data(df.toPandas(), "HELLOFRESHHELIOS", "HellofreshHelios_Sales")
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tenant", required=True)
    parser.add_argument('--extract_start_time', required=True,
                        type=lambda s: datetime.datetime.strptime(s, '%Y-%m-%dT%H:%M:%SZ'))
    parser.add_argument('--extract_end_time', required=True,
                        type=lambda s: datetime.datetime.strptime(s, '%Y-%m-%dT%H:%M:%SZ'))
    
    args, unknown_args = parser.parse_known_args()

    tenant = args.tenant
    extract_start_time = args.extract_start_time
    extract_end_time = args.extract_end_time
    
    app_name = "hellofresh_push_gamification_data"
    spark = get_spark_session(app_name, tenant)
    logger = gpc_utils_logger(tenant, app_name)


    try:
        push_sales(spark, extract_start_time, extract_end_time)
        logger.info("Gamification data pushed successfully")

    except Exception as e:
        logger.exception(e, stack_info=True, exc_info=True)
        raise