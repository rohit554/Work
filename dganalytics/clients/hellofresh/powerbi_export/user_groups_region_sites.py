from dganalytics.utils.utils import get_path_vars
from pyspark.sql import SparkSession
import os
import pandas as pd


def export_user_groups_region_sites(spark: SparkSession, tenant: str):

    tenant_path, db_path, log_path = get_path_vars(tenant)
    group_timezones = pd.read_csv(os.path.join(
        tenant_path, 'data', 'config', 'DG_Team_Group_Site_TimeZone_Mapping.csv'), header=0)
    group_timezones = spark.createDataFrame(group_timezones)
    group_timezones.registerTempTable("group_timezones")

    user_group_sites = spark.sql("""
        select userId, groupName, region, site, timeZone from (
	select 
	a.userId, a.groupName groupName, b.region, b.site, b.timeZone, 
	row_number() over(partition by a.userId order by a.groupName) as rn
	from gpc_hellofresh.dim_user_groups a, group_timezones b
	where a.groupName  = b.agentGroupName
	) where rn= 1

        """)
    user_group_sites.toPandas().to_csv(os.path.join(tenant_path, 'data', 'config', 'User_Group_region_Sites.csv'),
                                       header=True, index=False)
    df = user_group_sites.selectExpr(
        "userId as userKey", "groupName", "region", "site", "timeZone as time_zone")

    return df
