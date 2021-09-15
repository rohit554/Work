from dganalytics.utils.utils import get_path_vars
from pyspark.sql import SparkSession
import os
import pandas as pd


def export_user_groups_region_sites(spark: SparkSession, tenant: str, region: str):

    tenant_path, db_path, log_path = get_path_vars(tenant)
    '''
    group_timezones = pd.read_csv(os.path.join(
        tenant_path, 'data', 'config', 'DG_Team_Group_Site_TimeZone_Mapping.csv'), header=0)
    '''
    group_timezones = pd.read_json(os.path.join(
        tenant_path, 'data', 'config', 'DG_Team_Group_Site_TimeZone_Mapping.json'))
    group_timezones = pd.DataFrame(group_timezones['values'].tolist())
    header = group_timezones.iloc[0]
    group_timezones = group_timezones[1:]
    group_timezones.columns = header

    group_timezones = spark.createDataFrame(group_timezones)
    group_timezones.registerTempTable("group_timezones")

    user_group_sites = spark.sql("""
        SELECT  dug.userId,
                dug.groupName groupName,
                gt.region,
                gt.site,
                gt.timeZone
        FROM gpc_hellofresh.dim_user_groups dug
        INNER JOIN group_timezones gt
            ON LOWER(dug.groupName) = LOWER(gt.agentGroupName)
        """)
    user_group_sites.toPandas().to_csv(os.path.join(tenant_path, 'data', 'config', 'User_Group_region_Sites.csv'),
                                       header=True, index=False)
    df = user_group_sites.selectExpr(
        "userId as userKey", "groupName", "region", "site", "timeZone as time_zone")
    
    realtime_config = spark.sql("""
        SELECT  dug.groupName,
                dug.userId,
                du.userFullName AS name
        FROM gpc_hellofresh.dim_user_groups dug
        INNER JOIN group_timezones gt
            ON  LOWER(dug.groupName) = LOWER(gt.agentGroupName)
                AND LOWER(gt.region) = 'us'
        INNER JOIN gpc_hellofresh.dim_users du
            ON dug.userId = du.userId
        """).toPandas()
    realtime_config.to_csv(os.path.join(tenant_path, 'data', 'config', 'realtime_US_Users.csv'),
                           header=True, index=False)

    return df
