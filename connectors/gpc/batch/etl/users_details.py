import argparse
import requests as rq
import sys
from pyspark.sql import SparkSession, DataFrame
from connectors.gpc_utils import get_spark_session, env, gpc_request, get_path_vars, parser


if __name__ == "__main__":
    tenant, run_id, extract_start_date, extract_end_date = parser()
    tenant_path, db_path, log_path, db_name = get_path_vars(tenant, env)

    spark = get_spark_session(app_name="gpc_setup", env=env, tenant=tenant)

    print("gpc extract users", tenant)
    print("gpc extract users", run_id)

    df = gpc_request(spark, tenant, 'users_details', run_id, db_name, extract_start_date)
    # update_raw_table(spark, db_name, df)
