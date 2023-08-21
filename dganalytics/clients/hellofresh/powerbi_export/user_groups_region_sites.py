from dganalytics.utils.utils import get_path_vars
from pyspark.sql import SparkSession
import os
import pandas as pd


def export_user_groups_region_sites(spark: SparkSession, tenant: str, region: str):

    tenant_path, db_path, log_path = get_path_vars(tenant)
    group_timezones = pd.read_json(os.path.join(
        tenant_path, 'data', 'config', 'DG_Team_Group_Site_TimeZone_Mapping.json'))
    group_timezones = pd.DataFrame(group_timezones['values'].tolist())
    header = group_timezones.iloc[0]
    group_timezones = group_timezones[1:]
    group_timezones.columns = header

    group_timezones = spark.createDataFrame(group_timezones)
    group_timezones.createOrReplaceTempView("group_timezones")

    user_group_sites = spark.sql("""
        SELECT 
            userId, groupName, region, site, timeZone, provider
        FROM (
            SELECT 
                a.userId, a.groupName AS groupName, b.region, b.site, b.timeZone, b.Provider as provider,
                row_number() OVER (PARTITION BY a.userId ORDER BY a.groupName) AS rn
            FROM 
                gpc_hellofresh.dim_user_groups a
                JOIN group_timezones b ON lower(a.groupName) = lower(b.agentGroupName)
        )
        WHERE rn = 1

        """)
    user_group_sites.toPandas().to_csv(os.path.join(tenant_path, 'data', 'config', 'User_Group_region_Sites.csv'),
                                       header=True, index=False)
    df = user_group_sites.selectExpr(
        "userId as userKey", 
        "groupName", 
        "region", 
        "site", 
        "timeZone as time_zone")
    
    realtime_config = spark.sql("""
        SELECT 
            groupName, userId, name
        FROM (
            SELECT 
                a.userId, a.groupName AS groupName, b.region, b.site, b.timeZone, c.userFullName AS name,
                row_number() OVER (PARTITION BY a.userId ORDER BY a.groupName) AS rn
            FROM 
                gpc_hellofresh.dim_user_groups a
                JOIN group_timezones b ON lower(a.groupName) = lower(b.agentGroupName)
                JOIN gpc_hellofresh.dim_users c ON a.userId = c.userId
            WHERE
                lower(b.region) = 'us'
        )
        WHERE rn = 1

        """).toPandas()
    realtime_config.to_csv(os.path.join(tenant_path, 'data', 'config', 'realtime_US_Users.csv'),
                           header=True, index=False)

    return df
