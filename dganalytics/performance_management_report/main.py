from dganalytics.performance_management_report.queries import activity_mapping
from dganalytics.utils.utils import get_spark_session, env
from dganalytics.performance_management_report import queries
import argparse

pipelines = {
    "users": queries.get_users,
    "activity_wise_points": queries.get_activity_wise_points,
    "campaign": queries.get_campaign,
    "badges": queries.get_badges,
    "challenges": queries.get_challenges,
    "quizzes": queries.get_quizzes,
    "questions": queries.get_questions,
    "levels": queries.get_levels,
    "user_campaign": queries.get_user_campaign,
    "logins": queries.get_logins,
    "activity_mapping": queries.get_activity_mapping,
    "data_upload_audit_log": queries.get_data_upload_audit_log,
    "trek_data": queries.get_trek_data
}

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--pipeline', required=True)

    args, unknown_args = parser.parse_known_args()
    pipeline = args.pipeline

    tenant = "datagamz"
    app_name = "performance_management_calc"

    spark = get_spark_session(
        app_name=app_name, tenant=tenant, default_db='dg_performance_management')

    pipelines[pipeline](spark)
