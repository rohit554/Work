from dganalytics.utils.utils import get_spark_session, get_path_vars, get_secret
from typing import List
import requests
from pyspark.sql import SparkSession
from requests.auth import HTTPBasicAuth
import datetime
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BooleanType, DateType, FloatType
from pyspark.sql.functions import to_timestamp, from_utc_timestamp, expr, col, unix_timestamp, date_format, to_date
from datetime import datetime
import re
from pyspark.sql import functions as F
from dateutil import parser
from pyspark.sql import Column

def get_emailId(tenant: str):
    email = get_secret(f'{tenant}jirauserid')
    return email

def get_accesskey(tenant: str):
    api_key = get_secret(f'{tenant}jiraaccesskey')
    return api_key

def get_api_url(tenant: str, nextPageToken: str = None):
    url = get_secret(f'{tenant}jiraurl')
    url = "https://"+url+"/rest/api/3/search/jql?jql=project = CCMCBESC AND updated >= -1d ORDER BY updated ASC&maxResults=100&fields=*all" + ("&nextPageToken=" + nextPageToken if nextPageToken else "")
    return url

def jira_request(spark: SparkSession, email : str, api_key : str):
    app_name = "jira_project_issues"
    tenant = "hellofresh"
    db_name = f"dg_{tenant}"
    tenant_path, db_path, log_path = get_path_vars(tenant)
    spark = get_spark_session(app_name=app_name, tenant=tenant)
    auth = HTTPBasicAuth(email, api_key)
    entity = "issues"
    combined_data = []
    maxResults = 100
    req_type = "GET"
    
    schema = StructType([
                StructField("issueId", StringType(), True),
                StructField("issueKey", StringType(), True),
                StructField("projectId", StringType(), True),
                StructField("projectKey", StringType(), True),
                StructField("projectName", StringType(), True),
                StructField("customFieldId", StringType(), True),
                StructField("customerId", StringType(), True),
                StructField("requestId", StringType(), True),
                StructField("requestType", StringType(), True),
                StructField("resolutionName", StringType(), True),
                StructField("priority", StringType(), True),
                StructField("market", StringType(), True),
                StructField("brand", StringType(), True),
                StructField("assigneeName", StringType(), True),
                StructField("assigneeEmail", StringType(), True),
                StructField("assigneeIsactive", BooleanType(), True),
                StructField("reporterName", StringType(), True),
                StructField("reporterEmail", StringType(), True),
                StructField("reporterIsactive", BooleanType(), True),
                StructField("creatorName", StringType(), True),
                StructField("creatorEmail", StringType(), True),
                StructField("creatorIsactive", BooleanType(), True),
                StructField("status", StringType(), True),
                StructField("statusDescription", StringType(), True),
                StructField("statusCategoryName", StringType(), True),
                StructField("issueTypeDescription", StringType(), True),
                StructField("issueTypeName", StringType(), True),
                StructField("callbackReason", StringType(), True),
                StructField("createdDate", TimestampType(), True),
                StructField("updatedDate", TimestampType(), True), 
                StructField("resolutionDate", TimestampType(), True),
                StructField("genesysInteractionURL", StringType(), True),
                StructField("recordInsertDate", TimestampType(), True),
                StructField("orgId", StringType(), True)
            ])
    record_insert_date = datetime.now()
    nextPageToken = None
    while True:
        resp_list = []
        
        if req_type == "GET":
            url = get_api_url(tenant, nextPageToken)
            print(url)
            resp = requests.request(method='GET', url=url, auth=auth)
        else:
            raise Exception("Unknown request type in config")

        resp_json = resp.json()
        resp_list.extend(resp_json[entity])
        nextPageToken = resp_json.get('nextPageToken')
        issues = resp_json['issues']
        if not nextPageToken or not issues:
            break
        
        for item in resp_list:
            if 'fields' in item:
                fields = item['fields']
                status = None
                if 'status' in fields and 'name' in fields['status']:
                    status_name = fields['status']['name']

                status_description = None
                if 'status' in fields and 'description' in fields['status']:
                    status_description = fields['status']['description']

                status_category_name = None
                if 'status' in fields and 'statusCategory' in fields['status'] and 'name' in fields['status']['statusCategory']:
                    status_category_name = fields['status']['statusCategory']['name']

                project_id = None
                project_key = None
                project_name = None
                if 'project' in fields:
                    project = fields['project']
                    project_id = project.get('id')
                    project_key = project.get('key')
                    project_name = project.get('name')

                issue_type_description = None
                issue_type_name = None
                if 'issuetype' in fields:
                    issuetype = fields['issuetype']
                    issue_type_description = issuetype.get('description')
                    issue_type_name = issuetype.get('name')

                assignee = fields.get('assignee')
                assignee_email_address = None
                assignee_display_name = None
                isactive_assignee = None
                if assignee is not None:
                    assignee_email_address = assignee.get('emailAddress')
                    assignee_display_name = assignee.get('displayName')
                    isactive_assignee = assignee.get('active')

                resolutiondate = fields.get('resolutiondate')
                created = fields.get('created')
                if resolutiondate is not None:
                    resolutiondate = parser.isoparse(resolutiondate)
                if created is not None:
                    created = parser.isoparse(created)

                creator = fields.get('creator')
                creator_name = None
                creator_email = None
                isactive_creator = None
                if creator is not None:
                    creator_name = creator.get('displayName')
                    creator_email = creator.get('emailAddress')
                    isactive_creator = creator.get('active')

                reporter = fields.get('reporter')
                reporter_email_address = None
                reporter_display_name = None
                isactive_reporter = None
                if reporter is not None:
                    reporter_email_address = reporter.get('emailAddress')
                    reporter_display_name = reporter.get('displayName')
                    isactive_reporter = reporter.get('active')    

                customfield_15946_value = None
                customfield_15946 = fields.get('customfield_15946')
                if customfield_15946 is not None:
                    customfield_15946_value = customfield_15946.get('value')

                updated = fields.get('updated')
                if updated is not None:
                    updated = parser.isoparse(updated)

                priority_name = None
                if 'priority' in fields and 'name' in fields['priority']:
                    priority_name = fields['priority']['name']

                customfield_14355_value = fields.get('customfield_14355')
                orgId = tenant
                key_value = item.get('key')
                id_value = item.get('id')

                customfield_13697 = fields.get('customfield_13697')
                customfield_13697_value = None
                customfield_13697_id = None
                if customfield_13697 is not None:
                    customfield_13697_value = customfield_13697.get('value') if customfield_13697 is not None else None
                    customfield_13697_id = customfield_13697.get('id')

                if customfield_13697_value is None:
                    customfield_15947 = fields.get('customfield_15947')
                    customfield_15947_value = customfield_15947.get('value') if customfield_15947 is not None else None
                    customfield_13697_value = customfield_15947_value

                if customfield_13697_value:
                    values = customfield_13697_value.split()
                    market = values[0] if values else None
                    brand = values[1] if len(values) > 1 else None

                customfield_15945_value = None
                customfield_15945 = fields.get('customfield_15945')
                if customfield_15945 is not None:
                    if isinstance(customfield_15945, dict):
                        customfield_15945_value = customfield_15945.get('value')
                    else:
                        customfield_15945_value = str(customfield_15945)

                resolution_column = fields.get('resolution')
                resolution_name = resolution_column.get('name') if resolution_column else None

                customfield_11600 = fields.get('customfield_11600')
                if customfield_11600 is not None and 'requestType' in customfield_11600:
                    request_type = customfield_11600.get('requestType')
                    if request_type is not None:
                        request_id = request_type.get('id')
                        request_name = request_type.get('name')
                    else:
                        request_id, request_name = None, None
                else:
                    request_id, request_name = None, None

                combined_data.append({'issueId': id_value, 'issueKey': key_value, 'customFieldId': id, 'priority': priority_name, 'customFieldId': customfield_13697_id, 'customerId': customfield_15945_value, 'requestId': request_id, 'requestType': request_name, 'resolutionName': resolution_name, 'market': market, 'brand': brand, 'assigneeName': assignee_display_name, 'assigneeEmail': assignee_email_address, 'assigneeIsactive': isactive_assignee, 'reporterEmail': reporter_email_address, 'reporterName':reporter_display_name, 'reporterIsactive': isactive_reporter, 'status': status_name, 'statusDescription': status_description, 'statusCategoryName': status_category_name, 'projectId': project_id,'projectKey': project_key, 'projectName': project_name, 'issueTypeDescription': issue_type_description, 'issueTypeName': issue_type_name, 'callbackReason': customfield_15946_value, 'resolutionDate': resolutiondate, 'createdDate': created, 'updatedDate': updated, 'creatorName': creator_name, 'creatorEmail':creator_email, 'creatorIsactive':isactive_creator, 'recordInsertDate': record_insert_date, "genesysInteractionURL": customfield_14355_value, 'orgId': orgId})

        combined_df = spark.createDataFrame(combined_data, schema=schema)
        combined_df = combined_df.withColumn('created', to_date('createdDate'))
        combined_df = combined_df.dropDuplicates()
        combined_df = combined_df.createOrReplaceTempView("combined_data")

        spark.sql(f"""merge into {db_name}.raw_project_issues DB
                    using combined_data A
                    on A.issueId = DB.issueId
                    WHEN MATCHED THEN
                        UPDATE SET *
                    WHEN NOT MATCHED THEN
                        INSERT *
                    """)
        
    return True

if __name__ == '__main__':
    tenant = 'hellofresh'
    userid = get_emailId(tenant)
    accesskey = get_accesskey(tenant)
    jira_request(spark, userid, accesskey)