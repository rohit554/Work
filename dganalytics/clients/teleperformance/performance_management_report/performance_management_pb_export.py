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
            "pb_workspace": "92a9a9da-6078-4253-be21-deed20046b7d",
            "pb_roi_dataset": "74f7111b-257b-4dbd-8c12-750bf47e114f"
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
        },
        {
            "name": "walmart",
            "pb_workspace": "92a9a9da-6078-4253-be21-deed20046b7d",
            "pb_roi_dataset": "7a8ba40d-e2bc-4601-a4cf-32cfd0828523"
        },
        {
            "name": "adobe",
            "pb_workspace": "92a9a9da-6078-4253-be21-deed20046b7d",
            "pb_roi_dataset": "fb8907e4-50b2-4394-9b32-848351d63ca3"
        },
        {
            "name": "ingram",
            "pb_workspace": "",
            "pb_roi_dataset": ""
        },
        {
            "name": "intermedia",
            "pb_workspace": "",
            "pb_roi_dataset": ""
        },
        {
            "name": "tiktok",
            "pb_workspace": "",
            "pb_roi_dataset": ""
        },
        {
            "name": "captioncall",
            "pb_workspace": "92a9a9da-6078-4253-be21-deed20046b7d",
            "pb_roi_dataset": "c1ee55b4-6a6c-4fad-8524-642a5675fe28"
        },
        {
            "name": "captioncall",
            "pb_workspace": "",
            "pb_roi_dataset": ""
        },
        {
            "name": "icicidirect",
            "pb_workspace": "",
            "pb_roi_dataset": ""
        },
        {
            "name": "icicifastag",
            "pb_workspace": "",
            "pb_roi_dataset": ""
        },
        {
            "name": "nre",
            "pb_workspace": "",
            "pb_roi_dataset": ""
        },
        {
            "name": "apl",
            "pb_workspace": "",
            "pb_roi_dataset": ""
        },

    ]
    app_name = "performance_management_powerbi_export"

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
