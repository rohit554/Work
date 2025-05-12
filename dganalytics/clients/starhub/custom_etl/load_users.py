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
import re
from pyspark.sql.functions import to_date, date_format
from pyspark.sql.functions import expr

def get_lobs(lob: str = ""):
    if lobs is None or lob is None:
        return ""
    lob = lob.strip()
    if len(lobs) > 0 and 'LOB' in lobs.columns:
        matching_rows = lobs[lobs['LOB'] == lob].dropna()
        if len(matching_rows) > 0:
            return matching_rows.iloc[0]['Campaign']
    return ""

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_file', required=True)

    args, unknown_args = parser.parse_known_args()
    input_file = args.input_file

    tenant = 'datagamz'
    spark = get_spark_session('attendance_data', tenant)
    customer = 'starthub'
    db_name = f"dg_{customer}"
    tenant_path, db_path, log_path = get_path_vars(customer)

    mongoUsers = get_mongodb_users(customer.upper(), spark)
    mongoTeams = get_mongodb_teams(customer.upper(), spark)

    if input_file.endswith(".xlsx"):
        users = pd.read_excel(os.path.join(tenant_path, "data", "raw", "user_management", "user_management_test", input_file), skiprows=6, engine='openpyxl', usecols=lambda x: 'Unnamed: 0' not in x)
        
    elif input_file.endswith(".csv"):
        users = pd.read_csv(os.path.join(tenant_path, "data", "raw", "user_management", "user_management_test", input_file), skiprows=6, usecols=lambda x: 'Unnamed: 0' not in x)

    global lobs
    lobs = pd.read_csv(os.path.join(tenant_path, "data", "config", "campaing_lob_mapping.csv"))
    spark.conf.set("spark.sql legacy.timeParserPolicy", "LEGACY")
    spark.udf.register("get_lob_udf", get_lobs, StringType())

    users.rename(columns={'middle name': 'middle_name', 'license id': 'license_id', 'Full Name': 'Full_Name', 'Communication Email': 'Communication_Email', 'campaign': 'LOB'}, inplace=True)

    users.fillna({'password': '', 'gender': '', 'license_id': '', 'middle_name': '', 'contact_info.address': '', 'contact_info.city': '', 'contact_info.country': ''}, inplace=True)

    users['first_name'] = users['first_name'].apply(lambda x: re.sub(r'[^a-zA-Z0-9\s]', '', str(x)))
    users['last_name'] = users['last_name'].apply(lambda x: re.sub(r'[^a-zA-Z0-9\s]', '', str(x)))
    users['user_start_date'] = pd.to_datetime(users['user_start_date']).dt.strftime('%d-%m-%Y')
    users['last_working_date'] = pd.to_datetime(users['last_working_date']).dt.strftime('%d-%m-%Y').fillna('')
    users['dateofbirth'] = pd.to_datetime(users['dateofbirth']).dt.strftime('%d-%m-%Y').fillna('')
    users['team'] = users['team'].fillna('').astype(str).apply(lambda x: x.strip() + ' Team')

    users.insert(19, 'orgId', 'starthub')

    users = users.drop_duplicates()

    users = users[['password', 'first_name', 'middle_name','last_name', 'gender', 'user_id', 'user_start_date', 'email', 'dateofbirth', 'team', 'role', 'license_id', 'Full_Name', 'Communication_Email', 'LOB', 'last_working_date', 'orgId' ]]

    mongoUsers.createOrReplaceTempView("mongoUsers")
    mongoTeams.createOrReplaceTempView("mongoTeams")
 
    users= spark.createDataFrame(users)
    users = users.withColumn('user_start_date',
                         when(to_date('user_start_date', 'MM-dd-yyyy').isNotNull(),
                              date_format(to_date('user_start_date', 'MM-dd-yyyy'), 'dd-MM-yyyy')
                         ).otherwise(
                              when(to_date('user_start_date', 'dd-MM-yyyy').isNotNull(),
                                   date_format(to_date('user_start_date', 'dd-MM-yyyy'), 'dd-MM-yyyy')
                              ).otherwise('')
                         ).alias('user_start_date'))

    users.createOrReplaceTempView("users")

    spark.sql(f"""MERGE into {db_name}.starthub_user DB
                    USING users A
                    ON A.user_id = DB.user_id
                    and A.email = DB.email
                    WHEN MATCHED THEN
                    UPDATE SET *
                    WHEN NOT MATCHED THEN
                    INSERT *
                    """)

    updateUsersDF = spark.sql(f"""
            SELECT  '' password,
            MU.first_name,
            '' `middle name`,
            MU.last_name,
            CASE WHEN U.gender = 'NA' THEN '' ELSE U.gender END gender,
            MU.user_id,
            date_format(MU.user_start_date, 'dd-MM-yyyy') user_start_date,
            MU.email,
            '' `contact_info.address`,
            '' `contact_info.city`,
            '' `contact_info.country`,
            '' `dateofbirth`,
            U.team team,
            MU.role_id role,
            '' `license id`,
            MU.name `Full Name`,
            MU.communication_email `Communication Email`,
            get_lob_udf(U.LOB) campaign
    FROM mongoUsers MU
    INNER JOIN users U
        ON MU.user_id = U.user_id
        AND MU.email = U.email
    WHERE MU.role_id NOT IN ('Team Manager', 'Team Leader')
    AND (U.LOB IN ('Mobile GE') OR U.LOB IS NULL)
    """)
    
    createUsersDF = spark.sql(f"""
        SELECT  '' AS password,
            U.first_name,
            '' `middle name`,
            U.last_name,
            CASE WHEN U.gender = 'NA' THEN '' ELSE U.gender END gender,
            U.user_id,
            date_format(to_date(U.user_start_date, 'dd-MM-yyyy'), 'dd-MM-yyyy') user_start_date,
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
            WHERE NOT EXISTS (SELECT * FROM mongoUsers MU WHERE LOWER(MU.user_id) = LOWER(U.user_id))
                  AND TRIM(email) NOT IN ('-@datagamz.com', '@datagamz.com', 'Not Received@datagamz.com')
    """)
    
    if(createUsersDF.count() > 0):
        upload_gamification_users(createUsersDF.toPandas(), customer.upper())
    if(updateUsersDF.count() > 0):
        upload_gamification_users(updateUsersDF.toPandas(), customer.upper())

    deactivatedUsersDF = spark.sql(f"""
        SELECT
          user_id,
          email
        FROM
          {db_name}.starthub_user
        WHERE
          to_date(last_working_date, 'dd-MM-yyyy') < current_date()
    """)

    if deactivatedUsersDF.count() > 0:
        deactivate_gamification_users(deactivatedUsersDF.toPandas(), customer.upper())