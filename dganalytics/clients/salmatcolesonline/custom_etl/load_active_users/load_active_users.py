from dganalytics.utils.utils import get_spark_session, exec_mongo_pipeline, export_powerbi_csv, get_path_vars
from dganalytics.connectors.gpc.gpc_utils import get_dbname, gpc_utils_logger
from pyspark.sql.types import StructType, StructField, StringType, BooleanType
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os
from io import StringIO
from azure.storage.blob import BlobServiceClient

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
    users = get_active_users(spark)
    current_date = datetime.now().replace(day=1) - relativedelta(months=1)
    previous_month_start = current_date.replace(day=1)
    previous_month_end = current_date.replace(day=1) + relativedelta(days=-1)

    previous_month_start_str = previous_month_start.strftime('%Y-%m-%dT%H:%M:%SZ')
    previous_month_end_str = previous_month_end.strftime('%Y-%m-%dT%H:%M:%SZ')

    billingUsers = spark.sql(f"""
        SELECT DISTINCT
            U.userFullName AS name,
            U.state,
            U.userId,
            COALESCE(MU.role_id, "Agent") AS role_id,
            date_format(to_date('{previous_month_start_str}'), 'MMMM') AS month
        FROM (
            SELECT DISTINCT agentId
            FROM gpc_salmatcolesonline.dim_conversations
            WHERE conversationStart BETWEEN '{previous_month_start_str}' AND '{previous_month_end_str}'
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
                date_format(to_date('{previous_month_start_str}'), 'MMMM') AS month
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

    print("CSV file saved successfully.")