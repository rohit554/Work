
# COMMAND ----------

import requests
import json
import datetime

def test():
  

def authorize():auth_key = dbutils.secrets.get(scope=databrickssecretscope, key=genesysapikeysecretname)




# COMMAND ----------

auth_key = dbutils.secrets.get(scope=databrickssecretscope, key=genesysapikeysecretname)
# Genesys authorize to get access token
headers = {
  "Content-Type": "application/x-www-form-urlencoded",
  "Authorization": "Basic " + auth_key
}
auth_request = requests.post("https://login.mypurecloud.com/oauth/token?grant_type=client_credentials", headers=headers)
if auth_request.status_code != 200:
    print("Access token request failed.")
    raise Exception
access_token = auth_request.json()['access_token']
token_type = auth_request.json()['token_type']

# COMMAND ----------

# Set Access Token Auth Headers to request API
api_headers = {
  "Authorization": "Bearer {}".format(access_token),
  "Content-Type": "application/json"
}

# COMMAND ----------


def upload_blob(local_dir, remote_dir, file_name, container, updated_file_name = None):
  blob_client = BlobServiceClient.from_connection_string(dbutils.secrets.get(scope=databrickssecretscope, key=blobconnectionsecretname))
  container_client = blob_client.get_container_client(container)
  print(remote_dir)
  print(file_name)
  with open('{}/{}'.format(local_dir, file_name), 'rb') as f:
    container_client.upload_blob('{}/{}'.format(remote_dir, (file_name if updated_file_name is None else updated_file_name)), f, overwrite=True)

#def upload_blob_to_container(blob):
  

# COMMAND ----------

import os
def write_raw_json_to_blob(raw_json_list, api, start_time, end_time, client, tenant, source_system):
  """
    start_time ex: 2020-03-16T12:00:00
  """
  local_dir = '/tmp/{}/{}/{}/{}/raw/{}'.format(client, tenant, source_system, api, datetime.date.today().strftime('%Y-%m-%dT%H-%M-%S'))
  remote_dir = '{}/{}/raw/{}/{}/{}/{}'.format(client, tenant, start_time[0:4], start_time[5:7], start_time[8:10], start_time.replace(":", "-"))
  file_name = str(api) + "_" + start_time.replace(":", "-") + "_" + end_time.replace(":", "-") + ".json"
  os.makedirs(local_dir, exist_ok=True)
  with open('{}/{}'.format(local_dir, file_name), 'w+') as f:
    for item in raw_json_list:
      f.write(str(item) + "\n")
  upload_blob(local_dir, remote_dir, file_name, source_system)

# COMMAND ----------

import os
from os.path import isfile, join
from pyspark.sql import functions as f
def write_csv_to_blob(df, api, start_time, end_time, client, tenant, source_system):
  """
    start_time ex: 2020-03-16T12:00:00
  """
  # removing quotes and newline characters for stirng columns
  for c, t in df.dtypes:
    if t == 'string':
      df = df.withColumn(c, f.regexp_replace(f.regexp_replace(f.col(c), '"', ''),"\\n", "<>"))
      
  local_dir = '/tmp/{}/{}/{}/{}/flatten_csv/{}'.format(client, tenant, source_system, api, datetime.date.today().strftime('%Y-%m-%dT%H-%M-%S'))
  #print(local_dir)
  remote_dir = '{}/{}/flatten_csv/{}/{}/{}/{}'.format(client, tenant, start_time[0:4], start_time[5:7], start_time[8:10], start_time.replace(":", "-"))
  #print(remote_dir)
  os.makedirs(local_dir, exist_ok=True)
  df.repartition(1).write.csv(path='{}'.format(local_dir), mode='overwrite', header=True, compression='gzip', sep='|', emptyValue='', nullValue='')
  file_name = str(api) + "_" + start_time.replace(":", "-") + "_" + end_time.replace(":", "-") + ".csv.gz"
  outputfile = [f for f in os.listdir('/dbfs' + local_dir) if os.path.isfile(os.path.join('/dbfs' + local_dir, f)) and "part-" in f][0]
  print(outputfile)
  upload_blob('/dbfs' + local_dir, remote_dir, outputfile, source_system, file_name)
  

# COMMAND ----------

def convert_to_json(resp_list):
  resp_list = [json.dumps(l) for l in resp_list]
  return resp_list
               


# COMMAND ----------

def read_schema(schema_name, client, tenant):
  blob_client = BlobServiceClient.from_connection_string(dbutils.secrets.get(scope=databrickssecretscope, key=blobconnectionsecretname))
  container_client = blob_client.get_container_client(source_system)
  schema = json.loads(container_client.download_blob('source_api_schemas/{}.schema'.format(schema_name)).readall())
  from pyspark.sql.types import DataType, StructType
  schema = StructType.fromJson(schema)
  return schema

  