from dganalytics.utils.utils import get_spark_session, get_path_vars, upload_gamification_users, get_mongodb_users, get_mongodb_teams, deactivate_gamification_users
from dganalytics.utils.utils import get_spark_session, get_path_vars
import argparse
import pandas as pd
import os
import numpy as np
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import *
from datetime import datetime
from pyspark.sql.functions import unix_timestamp, from_unixtime

def get_lobs(lob: str = ""):
  lob = lob.strip()
  return lobs.where(lobs['LOB']==lob).dropna().iloc[0]['Campaign']

schema = StructType([StructField("password", StringType(), True),
                     StructField("first_name", StringType(), True),
                     StructField("middle_name", StringType(), True),
                     StructField("last_name", StringType(), True),
                     StructField("name", StringType(), True),
                     StructField("manager", StringType(), True),
                     StructField("gender", StringType(), True),
                     StructField("user_id", StringType(), True),
                     StructField("Emp_code", StringType(), True),
                     StructField("LDAP_ID", StringType(), True),
                     StructField("CCMS_ID", StringType(), True),
                     StructField("user_start_date", StringType(), True),
                     StructField("email", StringType(), True),
                     StructField("dateofbirth", StringType(), True),
                     StructField("team", StringType(), True),
                     StructField("role", StringType(), True),
                     StructField("Communication_Email", StringType(), True),
                     StructField("LOB", StringType(), True),
                     StructField("orgId", StringType(), True)
                    ])

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_file', required=True)

    args, unknown_args = parser.parse_known_args()
    input_file = args.input_file

    tenant = 'datagamz'
    app_name = 'airbnb_users'
    spark = get_spark_session(app_name = app_name, tenant=tenant, default_db='dg_performance_management')
    customer = 'airbnbprod'
    tenant_path, db_path, log_path = get_path_vars(customer)

    mongoUsers = get_mongodb_users(customer.upper(), spark)
    mongoTeams = get_mongodb_teams(customer.upper(), spark)

    if input_file.endswith(".xlsx"):
        users = pd.read_excel(os.path.join(tenant_path, "data", "raw", "user_management", input_file), sheet_name="EmpList", skiprows=2, engine='openpyxl', usecols=lambda x: 'Unnamed: 0' not in x)
        
    elif input_file.endswith(".csv"):
        users = pd.read_csv(os.path.join(tenant_path, "data", "raw", "user_management", input_file), skiprows=2, usecols=lambda x: 'Unnamed: 0' not in x)
        
    elif input_file.endswith(".xlsb"):
        xls = pd.ExcelFile(os.path.join(tenant_path, "data", "raw", "user_management", input_file))
        
        for sheet in xls.sheet_names:
            sheetDf = pd.read_excel(xls, sheet)
            frames = [users, sheetDf]
            users = pd.concat(frames)
    
    #global lobs 
    lobs = pd.read_csv(os.path.join(tenant_path, "data", "config", "campaing_lob_mapping.csv"))
    spark.conf.set("spark.sql legacy.timeParserPolicy", "LEGACY")
    spark.udf.register("get_lob_udf", get_lobs, StringType())

    name_parts = users['Name'].str.split(expand=True)
    users['first_name'], users['middle_name'], users['last_name'] = name_parts[0], '', name_parts[1]
    users = users.rename(columns={'Gender': 'gender'})
    users['gender'].fillna('Other', inplace=True)
    
    users = users.rename(columns={'Name': 'name'})
    users = users.rename(columns={'Manager': 'manager'})
    
    users = users.rename(columns={'Emp code': 'Emp_code'})
    users = users.rename(columns={'LDAP ID': 'LDAP_ID'})
    users = users.rename(columns={'CCMS ID': 'CCMS_ID'})
    
    users['DOJ'] = users['DOJ'].fillna(pd.Timestamp('now')).apply(lambda doj: datetime.now().strftime("%d-%m-%Y") if doj == 'DNA' else pd.to_datetime(doj).strftime('%d-%m-%Y'))

    users = users.rename(columns={'DOJ': 'user_start_date'})

    users['email'] = users['CCMS_ID'].replace(['-', '--', 'DNA', 'Profile not Active', 0, np.nan], '').apply(lambda x: x + "@teleperformancedibs.com" if len(str(x)) > 0 else '')

    users = users.rename(columns={'Supervisor': 'team'}).assign(team=lambda x: x["team"] + " Team")
    
    users = users.rename(columns={'Designation': 'role'})
    users['role'] = np.where(users['role'] == 'Ambassador', 'Agent', 
                         np.where(users['role'].isin(['Team Leader -  Operations', 'Trainer', 'Team Leader -  MIS']), 'Team Lead', 'Team Manager'))


    users = users.rename(columns={'Email ID': 'Communication_Email'})
    users['Communication_Email'] = np.where(users['Communication_Email'].isin(['-', '--', 'DNA']) | users['Communication_Email'].isna(), users['first_name'] + '.' + users['last_name'] + '@datagamz.com', users['Communication_Email'])

    users.insert(1, 'password', '')
    users.insert(6, 'user_id', users['CCMS_ID'])
    users.insert(9, 'contact_info.address', '')
    users.insert(10, 'contact_info.city', '')
    users.insert(11, 'contact_info.country', '')
    users.insert(12, 'dateofbirth', '')
    users.insert(15, 'license id', '')
    users.insert(16, 'Full Name', '')
    users.insert(17, 'orgId', 'airbnbprod')
    users = users[['password', 'first_name', 'middle_name', 'last_name', 'name', 'manager', 'gender', 'user_id', 'Emp_code', 'LDAP_ID', 'CCMS_ID', 'user_start_date', 'email', 'dateofbirth', 'team', 'role','Communication_Email', 'LOB', 'orgId']]
    users = users.astype(str) 
    users= spark.createDataFrame(users)

    users.createOrReplaceTempView("users")

    mongoUsers.createOrReplaceTempView("mongoUsers")
    mongoTeams.createOrReplaceTempView("mongoTeams")

    newDF = spark.sql(f"""DELETE FROM dg_airbnbprod.airbnb_user_data""")
    
    newDF = spark.sql(f"""INSERT INTO TABLE dg_airbnbprod.airbnb_user_data
                    SELECT A.password, A.first_name, A.middle_name, A.last_name, A.name, A.manager, A.gender, A.user_id, A.Emp_code, A.LDAP_ID, A.CCMS_ID, A.user_start_date, A.email, A.dateofbirth, A.team, A.role, A.Communication_Email, A.LOB, A.orgId
                    FROM users A
                    LEFT JOIN dg_airbnbprod.airbnb_user_data DB
                    ON A.user_start_date = DB.user_start_date
                    AND A.user_id = DB.user_id
                    AND A.email = DB.email
                    """)
    
    updateUsersDF = spark.sql(f"""
            SELECT  '' password,
            MU.first_name,
            '' `middle name`,
            MU.last_name,
            CASE WHEN U.gender = 'NA' THEN 'Other' ELSE U.gender END gender,
            MU.user_id,
            MU.user_start_date,
            MU.email,
            '' `contact_info.address`,
            '' `contact_info.city`,
            '' `contact_info.country`,
            '' `dateofbirth`,
            U.team team,
            u.role,
            '' `license id`,
            '' `Full Name`,
            MU.communication_email `Communication Email`,
            get_lob_udf(U.LOB) campaign
    FROM mongoUsers MU
    INNER JOIN mongoTeams MT
        ON MU.team_id = MT.team_id
    INNER JOIN users U
        ON LOWER(MU.user_id) = LOWER(U.user_id)
    WHERE U.LOB IN ('R1', 'R2', 'CE')
    """)
    
    
    createUsersDF = spark.sql(f"""
        SELECT  'Welcome@1234567' AS password,
            U.first_name,
            '' `middle name`,
            U.last_name,
            CASE WHEN U.gender = 'NA' THEN 'Other' ELSE U.gender END gender,
            U.user_id,
            U.user_start_date,
            U.email,
            '' `contact_info.address`,
            '' `contact_info.city`,
            '' `contact_info.country`,
            '' `dateofbirth`,
            U.team team,
            U.role,
            '' `license id`,
            '' `Full Name`,
            TRIM(U.`Communication_Email`) `Communication Email`,
            get_lob_udf(U.LOB) campaign
            FROM users U
            WHERE NOT EXISTS (SELECT 1 FROM mongoUsers MU WHERE LOWER(MU.user_id) = LOWER(U.user_id))
                  AND TRIM(email) NOT IN ('-@teleperformancedibs.com', '@teleperformancedibs.com', 'Not Received@teleperformancedibs.com')
                  AND U.LOB IN ('R1', 'R2', 'CE')
    """)
    
    
    if(createUsersDF.count() > 0):
        upload_gamification_users(createUsersDF.toPandas(), customer.upper())
    if(updateUsersDF.count() > 0):
        upload_gamification_users(updateUsersDF.toPandas(), customer.upper())
        
        
    deactivatedUsersDF = spark.sql(f"""
        SELECT MU.user_id, '' email FROM mongoUsers MU
        WHERE MU.role_id IN ('Agent') and (NOT EXISTS (SELECT 1 FROM Users U WHERE LOWER(U.user_id) = LOWER(MU.user_id) ))
    """)
    
    deactivate_gamification_users(deactivatedUsersDF.toPandas(), customer.upper())