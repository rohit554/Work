from dganalytics.utils.utils import get_spark_session, get_path_vars, export_powerbi_csv
from dganalytics.utils.utils import exec_powerbi_refresh
import argparse
import os

if __name__ == "__main__":
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument('--tenant_org_id', required=True)

    args, unknown_args = parser.parse_known_args()
    tenant_org_id = args.tenant_org_id
    '''
    tenant_org_id = 'hellofresh'

    app_name = "performance_management_powerbi_export"

    spark = get_spark_session(
        app_name=app_name, tenant=tenant_org_id, default_db='dg_performance_management')
    tables = ["activity_wise_points", "badges", "campaign", "challenges", "levels", "logins", "questions",
              "quizzes", "user_campaign", "users", "hellofresh_kpi_raw_data"]

    tenant_path, db_path, log_path = get_path_vars(tenant_org_id)
    for table in tables:
        df = spark.sql(
            f"select * from dg_performance_management.{table} where orgId = '{tenant_org_id}'")
        df = df.drop("orgId")
        export_powerbi_csv(tenant_org_id, df, f"pm_{table}")
    exec_powerbi_refresh('', '')
