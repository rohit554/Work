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
            "pb_workspace": "92a9a9da-6078-4253-be21-deed20046b7d",
            "pb_roi_dataset": "b9e11a5c-9817-4e67-bf79-3fa45458ec0e"
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
            "pb_workspace": "92a9a9da-6078-4253-be21-deed20046b7d",
            "pb_roi_dataset": "a6bb7cb9-1f4e-4c14-a56a-139e19ad254a"
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
            "name": "icicidirect",
            "pb_workspace": "92a9a9da-6078-4253-be21-deed20046b7d",
            "pb_roi_dataset": "2e11b65b-d8ca-47a2-a11a-226093ea84b5"
        },
        {
            "name": "icicifastag",
            "pb_workspace": "92a9a9da-6078-4253-be21-deed20046b7d",
            "pb_roi_dataset": "d4c60017-1640-4e86-9b46-204e0e4437c8"
        },
        {
            "name": "nre",
            "pb_workspace": "",
            "pb_roi_dataset": ""
        },
        {
            "name": "apl",
            "pb_workspace": "92a9a9da-6078-4253-be21-deed20046b7d",
            "pb_roi_dataset": "30e9a066-8d4a-4a65-a0c4-246dd53f015b"
        },
        {
            "name": "barclayhsc",
            "pb_workspace": "92a9a9da-6078-4253-be21-deed20046b7d",
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
