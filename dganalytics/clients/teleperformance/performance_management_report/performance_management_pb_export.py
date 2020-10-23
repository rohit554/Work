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
            "name": "bcp",
            "pb_workspace": "",
            "pb_roi_dataset": ""
        },
        {
            "name": "bob",
            "pb_workspace": "",
            "pb_roi_dataset": ""
        },
        {
            "name": "holden",
            "pb_workspace": "",
            "pb_roi_dataset": ""
        }
    ]
    app_name = "performance_management_powerbi_export"

    spark = get_spark_session(
        app_name=app_name, tenant='datagamz', default_db='dg_performance_management')
    tables = ["activity_wise_points", "badges", "campaign", "challenges", "levels", "logins", "questions",
              "quizzes", "user_campaign", "users", "tp_kpi_raw_data", "tp_campaign_activities"]
    for tenant in tenants:
        for table in tables:
            df = spark.sql(
                f"select * from dg_performance_management.{table} where orgId = '{tenant['name']}'")
            df = df.drop("orgId")
            export_powerbi_csv('tp' + tenant['name'] if tenant['name'] != 'holden' else 'holden', df, f"pm_{table}")
        exec_powerbi_refresh(tenant['pb_workspace'], tenant['pb_roi_dataset'])
