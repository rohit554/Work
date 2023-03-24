from dganalytics.utils.utils import get_spark_session, get_path_vars, export_powerbi_csv, exec_powerbi_refresh
import argparse
import os
import pandas as pd
if __name__ == "__main__":

    app_name = "performance_management_powerbi_export"

    tenant_path, db_path, log_path = get_path_vars('datagamz')
    tenants_df = pd.read_csv(os.path.join(
        tenant_path, 'data', 'config', 'PowerBI_ROI_DataSets_AutoRefresh_Config.csv'))
    tenants_df = tenants_df[tenants_df['platform'] == 'new']
    tenants = tenants_df.to_dict('records')

    spark = get_spark_session(
        app_name=app_name, tenant='datagamz', default_db='dg_performance_management')
    tables = ["activity_wise_points", "badges", "campaign", "challenges", "levels", "logins", "questions",
              "quizzes", "user_campaign", "users", "activity_mapping", "data_upload_audit_log", 
              "data_upload_connections", "kpi_data", "campaign_kpis", "trek_data"]
    for tenant in tenants:
        print(f"Getting ROI data for {tenant}")
        if 'hellofresh' in tenant['name']:
            continue
        tenant_path, db_path, log_path = get_path_vars(tenant['name'])

        for table in tables:

            print(f"extracting Table - {table}")
            df = spark.sql(
                f"select * from dg_performance_management.{table} where orgId = '{tenant['name']}'")
            print(f"table row count - {df.count()}")
            df = df.drop("orgId")
            export_powerbi_csv(tenant['name'], df, f"pm_{table}")

        exec_powerbi_refresh(
            tenant['pb_workspace'], tenant['pb_roi_dataset'])

    # for Hellofresh

    for table in tables:
        df = spark.sql(
            f"select * from dg_performance_management.{table} where orgId in ('hellofreshanz', 'hellofreshca', 'hellofreshuk', 'hellofreshus')")
        df = df.drop("orgId")
        export_powerbi_csv('hellofresh', df, f"pm_{table}")

    for rec in tenants_df[tenants_df['name'].str.contains('hellofresh')].to_dict('records'):
        exec_powerbi_refresh(rec['pb_workspace'], rec['pb_roi_dataset'])
