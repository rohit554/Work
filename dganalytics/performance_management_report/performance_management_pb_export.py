from dganalytics.utils.utils import get_spark_session, get_path_vars, export_powerbi_csv, exec_powerbi_refresh
import argparse
import os
import pandas as pd
import datetime
from datetime import datetime, timedelta
from dganalytics.performance_management_report.queries.logins import get_genesys_clients_attendance

def get_kpi_values_data(spark, orgId, orgIds):
    if orgIds == []:
        orgIds = ['-1']
        
    query = f"""
                WITH user_campaign_activities AS 
                (SELECT
                  d.report_date,
                  u.userId,
                  uc.campaignId,
                  ca.activityId activityId,
                  ca.entityName,
									ca.kpi_name,
                  ca.fieldName,
                  u.orgId
                FROM (SELECT EXPLODE(SEQUENCE(CAST("2022-10-01" AS DATE), CURRENT_DATE(), INTERVAL 1 day)) report_date) as d, dg_performance_management.users u
                JOIN dg_performance_management.user_campaign uc
                    ON u.orgId = uc.orgId AND u.mongoUserId = uc.userId
                JOIN dg_performance_management.campaign c
                    ON c.orgId = uc.orgId
                      AND c.campaignId = uc.campaignId
                      AND d.report_date BETWEEN c.start_date and c.endDate
                JOIN dg_performance_management.campaign_outcomes ca
                    ON ca.orgId = uc.orgId AND ca.campaignId = uc.campaignId
                WHERE
                  u.RoleId = 'Agent' AND (UC.orgId = '{orgId}' OR UC.orgId IN ({ ','.join(map(repr,orgIds))}))
                )

                SELECT
                  uca.userId,
                  date_format(uca.report_date, 'dd-MM-yyyy') as date,
                  uca.campaignId,
                  uca.activityId,
                  uca.entityName,
									uca.kpi_name,
                  uca.fieldName,
                  uca.orgId,
                  ap.points,
                  ap.target,
                  (CASE 
                    WHEN uca.entityName = 'Audit_Log' THEN l.loginAttempt 
                    WHEN uca.entityName = 'quiz' AND uca.fieldName = 'percentage_score' THEN q.quizPercentageScore 
                    WHEN uca.entityName = 'quiz' AND uca.fieldName = 'attempted_quiz' AND q.quizPercentageScore > 0 THEN 1 
                    WHEN kd.attr_value IS NULL OR kd.attr_value = '' THEN ap.fieldValue 
                    ELSE kd.attr_value END) AS fieldValue,
                  (CASE 
                    WHEN uca.entityName = 'quiz' AND uca.fieldName = 'percentage_score' THEN q.noOfCorrectQuestions * 100 
                    ELSE kd.num END) AS num,
                  (CASE 
                    WHEN uca.entityName = 'quiz' AND uca.fieldName = 'percentage_score' THEN totalQuestions 
                    WHEN kd.denom IS NULL THEN NULL 
                    ELSE kd.denom END) AS denom,
                  ap.noOfTimesPerformed
                FROM user_campaign_activities uca
                LEFT JOIN dg_performance_management.logins l 
                  ON uca.userId = l.userId AND uca.report_date = l.date
                LEFT JOIN dg_performance_management.quizzes q 
                  ON uca.userId = q.userId AND q.answeredDate = uca.report_date
                LEFT JOIN dg_performance_management.kpi_data kd 
                  ON kd.userId = uca.userId 
                    AND kd.report_date = uca.report_date
                    AND kd.connection_name = uca.entityName 
                    AND kd.attr_dict_key = uca.fieldName 
                LEFT JOIN dg_performance_management.activity_wise_points ap 
                  ON ap.userId = uca.userId 
                    AND ap.date = uca.report_date
                    AND ap.entityName = uca.entityName 
                    AND uca.fieldName = ap.fieldName 
                    AND ap.activityId = uca.activityId
                WHERE fieldValue IS NOT NULL OR points is NOT NULL
                AND (uca.orgId = '{orgId}' OR uca.orgId IN ({ ','.join(map(repr,orgIds))}))
    """
    return spark.sql(query)

def get_attendance_data(spark, orgId, orgIds):
  if orgIds == []:
    orgIds = ['-1']
  query = f"""
  SELECT 
  report_date reportDate,
  userId,
  COALESCE((COUNT(attr_value) > 0), FALSE) isPresent 
  FROM 
  dg_performance_management.kpi_data
  WHERE orgId = '{orgId}' OR orgId IN ({','.join(map(repr,orgIds))})
  GROUP BY userId,report_date,orgId  
  """
  return spark.sql(query)  

def store_genesys_clients_attendance(spark):
  extract_start_time = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
  for tenant in ['salmatcolesonline', 'skynzib', 'skynzob', 'hellofreshanz']:
    get_genesys_clients_attendance(spark,tenant, extract_start_time).createOrReplaceTempView("genesys_attendance")
    df=spark.sql(f"""
                SELECT to_date(actualStartTime, 'dd-MM-yyyy') AS reportDate,
                userId,
                actualStartTime loginTime,
                actualEndTime logoutTime,
                true as isPresent,
                '{tenant}' AS orgId,
                "{datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')}" AS recordInsertDate
                FROM genesys_attendance
                WHERE actualStartTime IS NOT NULL
                """)

    df.createOrReplaceTempView("attendance")
    spark.sql(f"""MERGE INTO dg_performance_management.attendance target
                    USING attendance source
                    ON date_format(cast(source.reportDate AS date), 'dd-MM-yyyy') = date_format(cast(target.reportDate AS date), 'dd-MM-yyyy')
                    AND source.userId = target.userId
                    AND source.orgId = target.orgId
                    AND source.loginTime = target.loginTime
                    AND source.logoutTime = target.logoutTime
                    WHEN MATCHED THEN
                        UPDATE SET *
                    WHEN NOT MATCHED THEN
                        INSERT *
                    """)

if __name__ == "__main__":

    app_name = "performance_management_powerbi_export"

    tenant_path, db_path, log_path = get_path_vars('datagamz')
    tenants_df = pd.read_csv(os.path.join(
        tenant_path, 'data', 'config', 'PowerBI_ROI_DataSets_AutoRefresh_Config.csv'))
    tenants_df = tenants_df[tenants_df['platform'] == 'new']
    tenants = tenants_df.to_dict('records')

    spark = get_spark_session(
        app_name=app_name, tenant='datagamz', default_db='dg_performance_management')
    store_genesys_clients_attendance(spark)
    tables = ["activity_wise_points", "badges", "campaign", "challenges", "levels", "logins", "questions",
              "quizzes", "user_campaign", "users", "activity_mapping", "data_upload_audit_log", 
              "data_upload_connections", "kpi_data", "campaign_kpis", "trek_data", "kpi_values", "attendance", "announcement"]
    for tenant in tenants:
        print(f"Getting ROI data for {tenant}")
        if 'hellofresh' in tenant['name']:
            continue
        tenant_path, db_path, log_path = get_path_vars(tenant['name'])

        for table in tables:

            print(f"extracting Table - {table}")
            if table == "kpi_values":
                df = get_kpi_values_data(spark, tenant['name'], [])
            elif table == "attendance" and tenant['name'] not in ['doordashprod','startekflipkartcx','tpindiait','airbnbprod', 'salmatcolesonline', 'skynzib', 'skynzob']:
                df = get_attendance_data(spark, tenant['name'], [])
            elif table == "attendance" and tenant['name'] in ['doordashprod','startekflipkartcx','tpindiait','airbnbprod']:
              continue
            else:
                df = spark.sql(
                    f"select * from dg_performance_management.{table} where orgId = '{tenant['name']}'")
                df = df.drop("orgId")
            print(f"table row count - {df.count()}")
            export_powerbi_csv(tenant['name'], df, f"pm_{table}")

        exec_powerbi_refresh(
            tenant['pb_workspace'], tenant['pb_roi_dataset'])

    # for Hellofresh

    for table in tables:
        if table == "kpi_values":
            df = get_kpi_values_data(spark, None, ['hellofreshanz', 'hellofreshus'])
        else:
            df = spark.sql(
                f"select * from dg_performance_management.{table} where orgId in ('hellofreshanz', 'hellofreshus')")
        export_powerbi_csv('hellofresh', df, f"pm_{table}")

    for rec in tenants_df[tenants_df['name'].str.contains('hellofresh')].to_dict('records'):
        exec_powerbi_refresh(rec['pb_workspace'], rec['pb_roi_dataset'])
