from dganalytics.utils.utils import get_spark_session, exec_mongo_pipeline, export_powerbi_csv, get_path_vars, exec_powerbi_refresh, get_secret
from dganalytics.connectors.gpc.gpc_utils import get_dbname, gpc_utils_logger
from pyspark.sql.types import StructType, StructField, StringType, BooleanType
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import os

def mongo_users(spark):
  schema = StructType([
      StructField("user_id", StringType(), True),
      StructField("name", StringType(), True),
      StructField("role_id", StringType(), True),
      StructField("is_active", StringType(), True)
  ])

  pipeline = [
      {"$match": {"org_id": "SALMATCOLESONLINE"}},
      {"$project": {"user_id": 1, "name": 1, "role_id": 1, "is_active": 1, "_id": 0}}
  ]

  users = exec_mongo_pipeline(spark, pipeline, 'User', schema)

  users.createOrReplaceTempView("users")
  
  return users
  

def spark_users(spark):
    users = mongo_users(spark)
    last_month_start_date = datetime.today().date().replace(day=1)-relativedelta(months=1)
    last_month_start_datetime = datetime.combine(last_month_start_date, datetime.min.time())
    last_month_start_datetime = last_month_start_datetime.strftime('%Y-%m-%dT%H:%M:%SZ')

    last_month_end_date = datetime.today().date().replace(day=1)
    last_month_end_datetime = datetime.combine(last_month_end_date, datetime.min.time())
    last_month_end_datetime = last_month_end_datetime.strftime('%Y-%m-%dT%H:%M:%SZ')

    billingUsers = spark.sql(f"""
        SELECT DISTINCT
            U.userFullName AS name,
            U.state,
            U.userId,
            COALESCE(MU.role_id, "Agent") AS role_id,
            date_format(to_date('{last_month_start_datetime}'), 'MMMM') AS month
        FROM (
            SELECT DISTINCT agentId
            FROM gpc_salmatcolesonline.dim_conversations
            WHERE conversationStart BETWEEN from_utc_timestamp('{last_month_start_datetime}', 'Australia/Melbourne') AND from_utc_timestamp('{last_month_end_datetime}', 'Australia/Melbourne')
        ) C
        INNER JOIN gpc_salmatcolesonline.dim_users U
            ON U.userId = C.agentId
        LEFT JOIN users MU
            ON MU.user_id = U.userId
        UNION
        (
            SELECT DISTINCT
                MU1.name,
                U1.state,
                MU1.user_id,
                MU1.role_id,
                date_format(to_date('{last_month_start_datetime}'), 'MMMM') AS month
            FROM users MU1
            INNER JOIN gpc_salmatcolesonline.dim_users U1
                ON U1.userId = MU1.user_id
                AND U1.state = 'active'
            WHERE
                MU1.is_active = true
                AND MU1.role_id IN ('Team Manager', 'Team Lead')
                AND MU1.name != 'Swapnil Gorghate'
        )
    """)

    return billingUsers


if __name__ == "__main__":
    tenant = 'datagamz'
    spark = get_spark_session('attendance_data', tenant)
    customer = 'salmatcolesonline'
    tenant_path, db_path, log_path = get_path_vars(customer)

    billingUsers = spark_users(spark)

    pandas_df = billingUsers.toPandas()
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    file_name = f"billing_users_{timestamp}.csv"

    file_path = os.path.join(tenant_path, "data", "pbdatasets", "billing_users", file_name)

    pandas_df.to_csv(file_path, index=False)
    exec_powerbi_refresh("73667350-ee9d-4e55-8d18-e1954b40c7a0", "97f7c57e-c7b0-444b-9bd7-7bd7e5fdd5b4")

    print("CSV file saved successfully.")