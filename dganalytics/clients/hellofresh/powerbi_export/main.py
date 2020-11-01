from dganalytics.utils.utils import get_spark_session, export_powerbi_csv
from dganalytics.connectors.gpc.gpc_utils import get_dbname
from dganalytics.clients.hellofresh import powerbi_export
import argparse

pipelines = {
    "conversation_metrics_daily_summary": powerbi_export.export_conversion_metrics_daily_summary,
    "users_primary_presence": powerbi_export.export_users_primary_presence,
    "user_groups_region_sites": powerbi_export.export_user_groups_region_sites,
    "users_routing_status": powerbi_export.export_users_routing_status,
    "users_info": powerbi_export.export_users_info,
    "user_roles": powerbi_export.export_user_roles,
    "evaluation_question_scores": powerbi_export.export_evaluation_question_scores
}

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--tenant', required=True)
    parser.add_argument('--run_id', required=True)
    parser.add_argument('--export_name', required=True)

    args, unknown_args = parser.parse_known_args()
    tenant = args.tenant
    run_id = args.run_id
    export_name = args.export_name
    db_name = 'gpc_hellofresh'

    app_name = "genesys_powerbi_extract"
    spark = get_spark_session(app_name, tenant, default_db=db_name)
    df = pipelines[export_name](spark, tenant)
    export_powerbi_csv(tenant, df, export_name)