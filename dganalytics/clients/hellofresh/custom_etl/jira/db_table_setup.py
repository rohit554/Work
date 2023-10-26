from dganalytics.utils.utils import get_spark_session, get_path_vars

app_name = "hf_jira_table_setup"
tenant = "hellofresh"
tenant_path, db_path, log_path = get_path_vars(tenant)
spark = get_spark_session(app_name=app_name, tenant=tenant)

spark.sql(f"""
                create database if not exists dg_{tenant} LOCATION '{db_path}/raw_project_issues'
            """)

spark.sql(f"""
        create table if not exists 
            dg_{tenant}.raw_project_issues
            (issueId string,
            issueKey string,
            projectId string,
            projectKey string,
            projectName string,
            customFieldId string,
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
            createdDate date,
            updatedDate timestamp,
            resolutionDate timestamp,
            genesysInteractionURL string,
            recordInsertDate timestamp,
            orgId string
            )
            using delta
            PARTITIONED BY (createdDate)
            LOCATION '{db_path}/dg_{tenant}/raw_project_issues'
            """)