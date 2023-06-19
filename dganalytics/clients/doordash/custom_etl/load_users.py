from dganalytics.utils.utils import get_spark_session, get_path_vars, upload_gamification_users, get_mongodb_users, get_mongodb_teams, deactivate_gamification_users
import argparse
import pandas as pd
import os
import numpy as np
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DateType
from pyspark.sql.functions import *

schema = StructType([StructField("user_id", StringType(), True),
                     StructField("Name", StringType(), True),
                     StructField("Communication Email", StringType(), True),
                     StructField("user_start_date", DateType(), True),
                     StructField("Current Phase", StringType(), True),
                     StructField("Designation", StringType(), True),
                     StructField("Location", StringType(), True),
                     StructField("LOB", StringType(), True),
                     StructField("Team Leader Name", StringType(), True),
                     StructField("Manager Name", StringType(), True),
                     StructField("NT ID/TP LAN ID", StringType(), True),
                     StructField("gender", StringType(), True),
                     StructField("LWD", DateType(), True),
                     StructField("first_name", StringType(), True),
                     StructField("last_name", StringType(), True),
                     StructField("email", StringType(), True),
                     StructField("team", StringType(), True),
                     StructField("campaign", StringType(), True)
                    
                    ])

def get_lobs(lob: str = ""):
    lob = lob.strip()
    if lob == "Doordash":
        return ','.join(lobs["Campaign"])
    return lobs.where(lobs['LOB']==lob).dropna().iloc[0]['Campaign']

def GetLastName(row):
    last_name = ''
    if((row['name1'] is None) and (row['name2'] is None) and (row['name3'] is None) and (row['name4'] is None)):
        return str(row['first_name'])
    
    if (row['name1'] is not None):
        last_name = last_name + ' ' + str(row['name1'])
    if (row['name2'] is not None):
        last_name = last_name + ' ' + str(row['name2'])
    if (row['name3'] is not None):
        last_name = last_name + ' ' + str(row['name3'])
    if (row['name4'] is not None):
        last_name = last_name + ' ' + str(row['name4'])
    return last_name.strip()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_file', required=True)

    args, unknown_args = parser.parse_known_args()
    input_file = args.input_file

    tenant = "datagamz"
    app_name = "doordash_users"
    customer = 'doordashprod'
    spark = get_spark_session(app_name=app_name, tenant=tenant, default_db='dg_performance_management')

    # Fetching Users from the MongoDB
    mongoUsers = get_mongodb_users(customer.upper(), spark)
    # Fetching Teams from the MongoDB
    mongoTeams = get_mongodb_teams(customer.upper(), spark)
    
    # Reading customer uploaded file
    tenant_path, db_path, log_path = get_path_vars(customer)
    users = pd.DataFrame()
    if input_file.endswith(".xlsx"):
        users = pd.read_excel(os.path.join(tenant_path, "data", "raw", "user_management", input_file), engine='openpyxl')
    elif input_file.endswith(".csv"):
        users = pd.read_csv(os.path.join(tenant_path, "data", "raw", "user_management", input_file))
    elif input_file.endswith(".xlsb"):
        xls = pd.ExcelFile(os.path.join(tenant_path, "data", "raw", "user_management", input_file))
        
        for sheet in xls.sheet_names:
            sheetDf = pd.read_excel(xls, sheet)
            frames = [users, sheetDf]
            users = pd.concat(frames)
    
    global lobs
    lobs = pd.read_csv(os.path.join(tenant_path, "data", 'config', "campaign_lob_mapping.csv"))
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    spark.udf.register("get_lob_udf", get_lobs, StringType())
    
    # Identifying user's first and last name using full name
    users = users.join(
        users['Name'].str.split(' ', expand=True).rename(columns={0:'first_name', 1:'name1', 2: 'name2', 3: 'name3', 4: 'name4'})
    )

    users["last_name"] = users.apply(lambda row: GetLastName(row), axis =1)
    users = users.drop(['name1', 'name2', 'name3', 'name4'], axis=1)

    # Handling the nan values
    users['LWD'] = users['LWD'].replace({np.nan: None})

    # Renaming columns based on the API template
    users.rename(columns = {'New Emp ID':'user_id', 'DOJ': 'user_start_date', 'Gender': 'gender', 'Email Id': 'Communication Email' }, inplace = True)
    # Generating email address based on the NT ID
    users['email'] = users['NT ID/TP LAN ID'].astype(str) + '@datagamz.com' # '@teleperformancedibs.com'
    users['team'] = users['Team Leader Name'].astype(str) + ' Team'
    users['team'] = users['team'].str.title()
    users['LWD'] = users['LWD'].replace({np.nan: None})
    mongoUsers.createOrReplaceTempView("mongoUsers")
    mongoTeams.createOrReplaceTempView("mongoTeams")

    users = spark.createDataFrame(users, schema)
    users.createOrReplaceTempView("users")

    updateUsersDF = spark.sql(f"""
            SELECT  '' password,
            MU.first_name,
            '' `middle name`,
            MU.last_name,
            CASE WHEN U.gender = 'NaN' THEN 'Other' ELSE U.gender END gender,
            MU.user_id,
            date_format(MU.user_start_date, 'dd-MM-yyyy') user_start_date,
            MU.email,
            '' `contact_info.address`,
            U.Location `contact_info.city`,
            '' `contact_info.country`,
            '' `dateofbirth`,
            CASE WHEN U.Designation IN ('Team Leader - Operations', 'Team Leader - WFM', 'TL Quality')
                    THEN CONCAT(U.Name, ' Team')
                 WHEN U.Designation IN ('Assistant Manager - Operations', 'Assistant Manager - Quality', 'Assistant Manager - WFM',
                                        'Assistant Manager - Training', 'Director', 'FPA', 'Manager Operations', 'Manager- WFM',
                                        'MIS', 'Quality Assistant Manager - MIS', 'Quality Analyst', 'RTA', 'Senior Manager - Quality',
                                       'Sr. Director', 'Trainer', 'Voice Coach', 'WFM')
                    THEN 'Doordash Team'
                 ELSE U.team END team,
            MU.role_id role,
            '' `license id`,
            MU.name `Full Name`,
            MU.communication_email `Communication Email`,
            get_lob_udf(U.LOB) campaign
    FROM mongoUsers MU
    INNER JOIN mongoTeams MT
        ON MU.team_id = MT.team_id
    INNER JOIN users U
        ON LOWER(MU.user_id) = LOWER(U.user_id)
            AND INITCAP(MT.name) != INITCAP(U.team)
    WHERE MU.role_id != 'Team Manager'
    AND U.LOB IN ('CX Chat Sendbird', 'Payment Pod', 'DX Chat Sendbird', 'Doordash')
    """)

    createUsersDF = spark.sql(f"""
        SELECT  'Welcome@1234567' AS password,
            U.first_name,
            '' `middle name`,
            U.last_name,
            CASE WHEN U.gender = 'NaN' THEN 'Other' ELSE U.gender END gender,
            U.user_id,
            date_format(U.user_start_date, 'dd-MM-yyyy') user_start_date,
            U.email,
            '' `contact_info.address`,
            U.Location `contact_info.city`,
            '' `contact_info.country`,
            '' `dateofbirth`,
            U.team team,
            CASE  WHEN U.Designation = 'Sr. Executive' THEN 'Agent'
                  WHEN U.Designation IN ('Team Leader - Operations', 'Team Leader - WFM', 'TL Quality') THEN 'Team Lead'
                  ELSE 'Team Manager'
            END role,
            '' `license id`,
            '' `Full Name`,
            TRIM(U.`Communication Email`) `Communication Email`,
            get_lob_udf(U.LOB) campaign
            FROM users U
            WHERE NOT EXISTS (SELECT 1 FROM mongoUsers MU WHERE LOWER(MU.user_id) = LOWER(U.user_id))
                  AND (U.LWD IS NULL OR U.LWD > CURRENT_DATE())
                  AND TRIM(email) NOT IN ('-@datagamz.com', '@datagamz.com', 'Not Received@datagamz.com')
                  AND U.LOB IN ('CX Chat Sendbird', 'Payment Pod', 'DX Chat Sendbird', 'Doordash')
            ORDER BY date_format(U.user_start_date, 'dd-MM-yyyy') DESC
    """)

    if(createUsersDF.count() > 0):
        upload_gamification_users(createUsersDF.toPandas(), customer.upper())
    if(updateUsersDF.count() > 0):
        upload_gamification_users(updateUsersDF.toPandas(), customer.upper())

    deactivatedUsersDF = spark.sql(f"""
        SELECT MU.user_id, '' email FROM mongoUsers MU
        WHERE role_id IN ('Agent', 'Team Lead', 'Team Manager') and (NOT EXISTS (SELECT 1 FROM Users U WHERE LOWER(U.user_id) = LOWER(MU.user_id) AND (U.LWD IS NULL OR U.LWD > CURRENT_DATE())))
        AND user_id NOT IN ('doordashprodmanager', '123445', '12345')
        AND MU.is_active
    """)

    deactivate_gamification_users(deactivatedUsersDF.toPandas(), customer.upper())