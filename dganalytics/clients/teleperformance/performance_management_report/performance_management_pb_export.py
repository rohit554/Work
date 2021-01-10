from dganalytics.utils.utils import get_spark_session, get_path_vars, export_powerbi_csv, exec_powerbi_refresh
import argparse
import os
import pandas as pd

if __name__ == "__main__":
    app_name = "performance_management_powerbi_export"

    tenant_path, db_path, log_path = get_path_vars('datagamz')
    tenants = pd.read_csv(os.path.join(tenant_path, 'data', 'config', 'PowerBI_ROI_DataSets_AutoRefresh_Config.csv'))
    tenants = tenants[tenants['platform'] == 'old']
    tenants = tenants.to_dict('records')
    spark = get_spark_session(
        app_name=app_name, tenant='datagamz', default_db='dg_performance_management')
    tables = ["activity_wise_points", "badges", "campaign", "challenges", "levels", "logins", "questions",
              "quizzes", "user_campaign", "users", "tp_kpi_raw_data", "tp_campaign_activities"]
    for tenant in tenants:
        print(f"generatin ROI csv files for {tenant}")
        for table in tables:
            df = spark.sql(
                f"select * from dg_performance_management.{table} where orgId = '{tenant['name']}'")
            df = df.drop("orgId")
            export_powerbi_csv('tp' + tenant['name'] if tenant['name'] != 'holden' else 'holden', df, f"pm_{table}")
        exec_powerbi_refresh(tenant['pb_workspace'], tenant['pb_roi_dataset'])
