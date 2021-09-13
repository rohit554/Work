from dganalytics.utils.utils import get_spark_session, export_powerbi_parquet
from dganalytics.clients.hellofresh import powerbi_export
import argparse
import os

pipelines = {
    "user_groups_region_sites": powerbi_export.export_user_groups_region_sites,
    "wrapup_codes": powerbi_export.export_wrapup_codes,
    "routing_queues": powerbi_export.export_routing_queues,
    "evaluation_forms_answer_options": powerbi_export.export_evaluation_forms_answer_options,
    "evaluation_forms_questions": powerbi_export.export_evaluation_forms_questions,
    "evaluation_details_info": powerbi_export.export_evaluation_details_info,
    "evaluation_question_scores": powerbi_export.export_evaluation_question_scores,
    "users_routing_status_sliced": powerbi_export.export_users_routing_status_sliced,
    "user_roles": powerbi_export.export_user_roles,
    "users_info": powerbi_export.export_users_info,
    "users_primary_presence": powerbi_export.export_users_primary_presence,
    "user_presence": powerbi_export.export_user_presence,
    "conversation_metrics_daily_summary": powerbi_export.export_conversion_metrics_daily_summary,
    "wfm_day_metrics": powerbi_export.export_wfm_day_metrics,
    "wfm_actuals": powerbi_export.export_wfm_actuals,
    "survey_summary": powerbi_export.export_survey_summary
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
    df = pipelines[export_name](spark, tenant, 'US')
    export_powerbi_parquet(tenant, df, os.path.join('US', export_name))

    df = pipelines[export_name](spark, tenant, 'nonUS')
    export_powerbi_parquet(tenant, df, os.path.join('nonUS', export_name))
