from dganalytics.utils.utils import get_spark_session, export_powerbi_csv, get_path_vars
from pyspark.sql import SparkSession
from dganalytics.connectors.gpc.gpc_utils import pb_export_parser, get_dbname, gpc_utils_logger
import os


def export_user_groups_region_sites(spark: SparkSession, tenant: str):

    tenant_path, db_path, log_path = get_path_vars(tenant)
    group_timezones = spark.read.option("header", "true").csv(
        os.path.join(tenant_path, 'data', 'config', 'DG_Team_Group_Site_TimeZone_Mapping.csv'))
    group_timezones.registerTempTable("group_timezones")

    user_group_sites = spark.sql("""
        select userId, groupName, region, site, timeZone from (
        select 
            ug.userId, ug.groupName, gt.region, gt.site, gt.timeZone,
            row_number() over(partition by userId order by groupName) as rn
         from 
            (
                select userId, a.groups.id as groupId, b.name groupName from 
                    (select id userId, explode(groups) as groups  from gpc_hellofresh.raw_users ) a,
                    gpc_hellofresh.raw_groups b
                    where a.groups.id = b.id
                    and lower(b.name) like 'dg team%'
            ) ug, group_timezones gt
            where ug.groupName = gt.agentGroupName
            ) where rn = 1
        """)
    user_group_sites.toPandas().to_csv(os.path.join(tenant_path, 'data', 'config', 'User_Group_region_Sites.csv'),
                                       header=True, index=False)
    df = user_group_sites.selectExpr("userId", "userId as userKey", "groupName", "region", "site", "timeZone as time_zone")

    return df