from pyspark.sql import SparkSession
from dganalytics.utils.utils import get_path_vars
import os
import pandas as pd


def raw_project_issues(spark: SparkSession, tenant: str, region: str):
    df = spark.sql(f"""
                   select              
                    a.issueId,
                    a.issueKey,
                    a.projectId,
                    a.projectKey,
                    a.projectName,
                    a.customFieldId,
                    a.priority,
                    a.market,
                    a.brand,
                    a.assigneeName,
                    a.assigneeEmail,
                    a.assigneeIsactive,
                    a.reporterName,
                    a.reporterEmail,
                    a.reporterIsactive,
                    a.creatorName,
                    a.creatorEmail,
                    a.creatorIsactive,
                    a.status,
                    a.statusDescription,
                    a.statusCategoryName,
                    a.issueTypeDescription,
                    a.issueTypeName,
                    a.callbackReason,
                    date_format(a.createdDate, "yyyy-MM-dd'T'HH:mm:ss'Z'") as createdDate,
                    date_format(a.updatedDate, "yyyy-MM-dd'T'HH:mm:ss'Z'") AS updatedDate,
                    date_format(a.resolutionDate, "yyyy-MM-dd'T'HH:mm:ss'Z'") AS resolutionDate,
                    a.genesysInteractionURL,
                    a.recordInsertDate,
                    a.orgId
                  FROM
                    dg_hellofresh.raw_project_issues a
                    """)
    return df