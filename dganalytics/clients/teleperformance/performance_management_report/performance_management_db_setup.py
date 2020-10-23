
from dganalytics.utils.utils import get_spark_session, get_path_vars


app_name = "datagamz_performance_management_setup"
tenant = "datagamz"
tenant_path, db_path, log_path = get_path_vars(tenant)
spark = get_spark_session(app_name=app_name, tenant=tenant)

spark.sql(f"""
        create table if not exists 
            dg_performance_management.tp_kpi_raw_data
            (
                campaignId string,
                kpi string,
                outcomeName string,
                outcomeId string,
                userId string,
                date date,
                value float,
                orgId string
            )
            using delta
            PARTITIONED BY (orgId, date)
            LOCATION '{db_path}/dg_performance_management/tp_kpi_raw_data'
        """)

spark.sql(f"""
        create table if not exists 
            dg_performance_management.tp_campaign_activities
            (
                campaignId string,
                activityId string,
                activityName string,
                kpiName string,
                isChallengeActivity boolean,
                orgId string
            )
            using delta
            PARTITIONED BY (orgId)
            LOCATION '{db_path}/dg_performance_management/tp_campaign_activities'
        """)
