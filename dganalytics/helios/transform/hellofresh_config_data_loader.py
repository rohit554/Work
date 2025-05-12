import pandas as pd
import os
import json
from dganalytics.utils.utils import get_path_vars, get_spark_session
import argparse
from dganalytics.clients.hellofresh.export_gamification_data.hellofresh_push_gamification import get_config_file_data, get_config_csv_file_data


def load_queue_language(spark, tenant):
    tenant_path = get_path_vars(tenant)[0]
    
    data = get_config_file_data(tenant_path, "outboundQueueLanguage.json")
    queue_language = spark.createDataFrame(data)
    queue_language = queue_language.withColumnRenamed("Language ", "Language")

    queue_language.createOrReplaceTempView("queue_language")

    df = spark.sql(
        f"""
                  insert overwrite dgdm_{tenant}.dim_queue_language
                  select distinct Region, Brand, Queues queueName, ql.Language  language, queueId from queue_language ql
                        left join dgdm_{tenant}.dim_queues dq on lower(trim(ql.Queues)) = lower(trim(dq.queueName))
                  """
    )
    spark.sql("""insert overwrite dgdm_hellofresh.mv_queue_language select * from gpc_hellofresh.dim_queue_language""")

def load_queue_region_timezone_mapping(spark, tenant):
    tenant_path =get_path_vars(tenant)[0]
    data = get_config_file_data(tenant_path, "Queue_TimeZone_Mapping_v2.json")
    queue_timezones = spark.createDataFrame(data)
    queue_timezones.createOrReplaceTempView("queue_timezones")
    df=spark.sql(f"""
                insert overwrite dgdm_{tenant}.dim_queue_region_mapping
                select distinct dq.queueName,
                dq.queueId,
                region,country,
                brand,
                language,timeZone
                    from
                (select queueName,region,country,split(`Country-Brand-Language`,'-')[1] brand,
                split(`Country-Brand-Language`,'-')[2] language, timeZone 
                from queue_timezones) q
                join dgdm_{tenant}.dim_queues dq on
                lower(trim(q.queueName)) = lower(trim(dq.queueName))
                
                
                """)
def load_user_group_region_mapping(spark,tenant):
    tenant_path = get_path_vars(tenant)[0]
    user_df = get_config_csv_file_data(tenant_path, 'User_Group_region_Sites.csv')
    user_df = spark.createDataFrame(user_df)
    user_df.createOrReplaceTempView("user_group_region_sites")
    df1 = spark.sql(f"""
                    insert overwrite dgdm_{tenant}.dim_user_group_region_mapping
                    select distinct
                    userId,
                    groupName,
                    region,
                    region country,
                    timeZone,
                    provider
                    from user_group_region_sites
                """)

def load_outbound_wrapcodes(spark,tenant):
    tenant_path = get_path_vars(tenant)[0]
    outbound_wrapups = pd.read_csv(os.path.join(tenant_path, 'data', 'config', 'Outbound_Wrapup_Codes.csv'))
    outbound_wrapups = spark.createDataFrame(outbound_wrapups)
    outbound_wrapups.createOrReplaceTempView("outbound_wrap_codes")
    df1 = spark.sql(f"""
                    insert overwrite dgdm_{tenant}.dim_outbound_wrapup_codes
                    select * from outbound_wrap_codes
                """)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tenant", required=True)

    args, unknown_args = parser.parse_known_args()
    tenant = args.tenant
    spark = get_spark_session(app_name="Hellofresh Config data Loading To Table", tenant=tenant)
    if tenant == 'hellofresh':
          load_queue_language(spark, tenant)
          load_queue_region_timezone_mapping(spark, tenant)
          load_user_group_region_mapping(spark,tenant)
          load_outbound_wrapcodes(spark,tenant)
