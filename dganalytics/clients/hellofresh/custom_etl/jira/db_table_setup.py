import argparse
from pyspark.sql import SparkSession
from dganalytics.utils.utils import get_spark_session, get_path_vars

def create_database(spark: SparkSession, path: str, db_name: str):
    spark.sql(
        f"create database if not exists {db_name}  LOCATION '{path}/{db_name}'")

    return True

def create_jira_project_issues_table(spark: SparkSession, db_name: str, db_path: str):
    spark.sql(f"""
        create table if not exists 
            dg_{tenant}.raw_project_issues
            (issueId string,
            issueKey string,
            projectId string,
            projectKey string,
            projectName string,
            customFieldId string,
            customerId string,
            requestId string, 
            requestType string,
            resolutionName string,
            priority string,
            market string,
            brand string,
            assigneeName string,
            assigneeEmail string,
            assigneeIsactive boolean,
            reporterName string,
            reporterEmail string,
            reporterIsactive boolean,
            creatorName string,
            creatorEmail string,
            creatorIsactive boolean,
            status string,
            statusDescription string,
            statusCategoryName string,
            issueTypeDescription string,
            issueTypeName string,
            callbackReason string,
            createdDate timestamp,
            updatedDate timestamp,
            resolutionDate timestamp,
            genesysInteractionURL string,
            recordInsertDate timestamp,
            orgId string,
            created date
            )
            using delta
            PARTITIONED BY (created)
            LOCATION '{db_path}/dg_{tenant}/raw_project_issues'
            """)
    return True

if __name__ == "__main__":
    tenant = "hellofresh"
    db_name = f"dg_{tenant}"
    tenant_path, db_path, log_path = get_path_vars(tenant)
    spark = get_spark_session(app_name="hf_jira_table_setup",
                              tenant=tenant, default_db=db_name)
    create_database(spark, db_path, db_name)
    create_jira_project_issues_table(spark, db_name, db_path)