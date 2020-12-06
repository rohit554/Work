from dganalytics.utils.utils import get_spark_session, get_path_vars, export_powerbi_csv, exec_powerbi_refresh
import argparse
import os

if __name__ == "__main__":
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument('--tenant_org_id', required=True)

    args, unknown_args = parser.parse_known_args()
    tenant_org_id = args.tenant_org_id
    '''

    tenants = [
        {
            "name": "salmatcolesonline",
            "pb_workspace": "92a9a9da-6078-4253-be21-deed20046b7d",
            "pb_roi_dataset": "8db1907d-47aa-408e-a196-42c15218fb6e"
        },
        {
            "name": "atnt",
            "pb_workspace": "92a9a9da-6078-4253-be21-deed20046b7d",
            "pb_roi_dataset": "d3dbc8fb-6765-4c8d-9c01-6039fb4693a0"
        }
    ]
    app_name = "performance_management_powerbi_export"

    spark = get_spark_session(
        app_name=app_name, tenant='datagamz', default_db='dg_performance_management')
    tables = ["activity_wise_points", "badges", "campaign", "challenges", "levels", "logins", "questions",
              "quizzes", "user_campaign", "users", "activity_mapping"]
    for tenant in tenants:
        tenant_path, db_path, log_path = get_path_vars(tenant['name'])
        for table in tables:
            df = spark.sql(
                f"select * from dg_performance_management.{table} where orgId = '{tenant['name']}'")
            df = df.drop("orgId")
            export_powerbi_csv(tenant['name'], df, f"pm_{table}")
        exec_powerbi_refresh(tenant['pb_workspace'], tenant['pb_roi_dataset'])
