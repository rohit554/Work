from dganalytics.utils.utils import get_logger
from dganalytics.helios.transform.scripts import *
import os

def helios_utils_logger(tenant, app_name):
  global logger
  logger = get_logger(tenant, app_name)
  return logger
 
def read_helios_transform_sql_query(spark, transformation, tenant, extract_date, extract_start_time, extract_end_time, interaction_type):
  logger = helios_utils_logger(tenant,"helios")
  if interaction_type == "insert" :
    sql_file_path = os.path.join(os.path.abspath(os.path.dirname('__file__')),"scripts",interaction_type+"_query.sql")
  else:
    sql_file_path = os.path.join(os.path.abspath(os.path.dirname('__file__')),"scripts",transformation+"_"+interaction_type+".sql")
  
  try:
    # Read SQL query from file
    with open(sql_file_path, 'r') as file:
      sql_query = file.read()
    formatted_sql_query = sql_query.format(
          tenant=tenant,
          extract_date=extract_date,
          extract_start_time=extract_start_time,
          extract_end_time=extract_end_time,
          transformation = transformation
      )
    return formatted_sql_query
  except Exception as e:
    logger.exception(f"Error Occured in reading helios transformation SQL Query for {extract_start_time}_{extract_end_time}_{tenant}_{transformation}_{sql_file_path}")
    logger.exception(e, stack_info=True, exc_info=True)
    raise Exception



