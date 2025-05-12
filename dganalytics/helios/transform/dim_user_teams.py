from dganalytics.utils.utils import get_path_vars
from pyspark.sql import SparkSession
import os
import pandas as pd
from pyspark.sql.functions import col, when
from datetime import datetime, timedelta
import argparse

def hf_dim_user_teams(tenant):
    
    tenant_path, db_path, log_path = get_path_vars(tenant)
    group_timezones = pd.read_csv(os.path.join(tenant_path, 'data', 'config', 'User_Group_region_Sites.csv'))

    # group_timezones.display()
    group_timezones =  spark.createDataFrame(group_timezones)
    group_timezones.createOrReplaceTempView("User_Group_region_Sites")

    df = spark.sql(f"""
    SELECT 
        dug.userId, 
        ud.name as userFullName,
        dug.groupId, 
        ugrs.groupName,
        ugrs.groupName teamName, 
        concat_ws(',', collect_list(ud.roles.name)) as role , 
        ugrs.region, 
        ugrs.site, 
        ugrs.timeZone, 
        ugrs.provider, 
        case when ud.employerInfo.dateHire is null then '2020-01-01' else ud.employerInfo.dateHire end as startDate, 
        case when ud.state='active' then '9999-12-31' else max(ud.extractDate) end as endDate,
        case when ud.state='active' then true else false end as isActive
    FROM 
        User_Group_region_Sites ugrs
        JOIN gpc_{tenant}.dim_user_groups dug 
        ON lower(dug.groupName) = lower(ugrs.groupName)
            AND ugrs.userId= dug.userId
        JOIN (select *, explode(authorization.roles) roles  from gpc_{tenant}.raw_users) ud
        ON ud.id = ugrs.userId
        AND ud.id=dug.userId
        group by dug.userId,name,groupId,dug.groupName,teamName,region,site,timeZone,provider,employerInfo.dateHire,state
    """)

    # Execute the SQL update query using a cursor loop
    df.createOrReplaceTempView("dim_user_teams")
    rows = spark.sql(f"SELECT * FROM dim_user_teams WHERE isActive = true").collect()
    yesterday_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    today_date= datetime.now().strftime('%Y-%m-%d')

    # Update rows where isActive is true and startDate matches today_date
    # spark.sql(f"""
    #     UPDATE dgdm_{tenant}.dim_user_teams AS t1
    #     SET 
    #         endDate = current_date() - 1,
    #         isActive = false
    #     WHERE EXISTS (SELECT 1 FROM dim_user_teams t2 WHERE t1.userId = t2.userId AND t2.isActive AND (t1.groupId != t2.groupId OR lower(t1.groupName) != lower(t2.groupName) OR lower(t1.teamName) != lower(t2.teamName) OR array_sort(split(t1.role,',')) != array_sort(split(t2.role,',')) ) )
    # """)
    # user_list=spark.sql("select distinct userId from dgdm_{tenant}.dim_user_teams where endDate=current_date() - 1 and !isActive").collect()

    # for user in user_list:
    #   print(user['userId'])
    #   df=df.withColumn("startDate", when((col("userId") == user['userId']) & (col("isActive")), today_date).otherwise(col('startDate')))
    # df.display()
    df.createOrReplaceTempView("dim_user_teams_new")

    spark.sql(f"""
        insert overwrite dgdm_{tenant}.dim_user_teams (userId,userFullName,groupId,groupName,teamName,role,region,site,timeZone,provider,startDate,endDate,isActive)
        SELECT * FROM dim_user_teams_new a                    
    """)

def simplyenergy_dim_user_teams(tenant):
    spark.sql(f"""
    INSERT OVERWRITE dgdm_{tenant}.dim_user_teams (userId, userFullName, groupId, groupName, teamName)        
    SELECT u.id AS userId, du.userFullName, u.managementId AS groupId, mu.name AS groupName, mu.name AS teamName
        FROM gpc_{tenant}.raw_management_unit_users u
        JOIN gpc_{tenant}.raw_management_units mu ON u.managementId = mu.id
        JOIN dgdm_{tenant}.dim_users du ON u.id = du.userId
    """)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tenant", required=True)
    args, unknown_args = parser.parse_known_args()
    tenant = args.tenant

    if tenant == "simplyenergy":
        simplyenergy_dim_user_teams(tenant)
    elif tenant == "hellofresh":
        hf_dim_user_teams(tenant)
    else:
        print("Invalid Tenant")