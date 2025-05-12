import argparse
from dganalytics.utils.utils import get_secret, get_path_vars, get_spark_session
from io import StringIO
import logging
import os
import pandas as pd
import py7zr

def extract7zFiles(file_name, tenant, tenant_path):
	# Write logic to not process the file if it is already processed
	if not file_name.endswith(".7z"):
		logging.info("Incorrect file extension")
		return None
	password = get_secret(f"{tenant}filePassword")
	extractPath = os.path.join(tenant_path, 'data', 'raw', 'extractedFiles', f'{file_name.split(".")[0].replace(" ", "_").replace("-", "_")}')
	logging.info("extracting files")
	with py7zr.SevenZipFile(os.path.join(tenant_path, 'data', 'raw', 'CompressedFiles', file_name), mode='r', password=password) as z:
		z.reset()
		z.extractall(extractPath)
	return extractPath

def listAllFiles(extractPath):
	return os.listdir(extractPath)

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--tenant', required=True)
	parser.add_argument('--file_name', required=True)
	args, unknown_args = parser.parse_known_args()
	tenant = args.tenant
	file_name = args.file_name

	tenant_path, db_path, log_path = get_path_vars(tenant)
	app_name = "eGainSFTP"
	db_name = f"egain_{tenant}"
	spark = get_spark_session(app_name, tenant, default_db=db_name)
	try:
		extractPath = extract7zFiles(file_name, tenant, tenant_path)
		if extractPath is not None:
			tables = ["EGPL_USER", "EGSS_SESSION", "EGSS_SESSION_ENTRY", "EGPL_EVENT_HISTORY_KB", "EGPL_EVENT_HISTORY_USER", "EGPL_KB_ARTICLE"]

			for file in listAllFiles(extractPath):
				# Manifest files
				if file.startswith("manifest_file"):
					manifest = pd.read_csv(os.path.join(extractPath, file), encoding='UTF-16 LE', quotechar="'")
					manifest_cols = ["Sr No", "dataType", "minLength", "maxLength", "endsWith", "sequence", "fieldName", "encoding"]
					print("Looping through tables")
					for table in manifest['TABLE_NAME'].unique():
						if table not in tables:
							continue
						# Get format for each table
						format = pd.read_csv(os.path.join(extractPath, f"{table}_Format.fmt"), skiprows=2, header=None, delimiter=r"\s+", names=manifest_cols)
				
						# Getting all the file names for the table
						tab_files = manifest[manifest['TABLE_NAME'] == table]['FILE_NAME']
						exportType = manifest[manifest['TABLE_NAME'] == table]['EXPORT_TYPE'].unique()
						print(f"Processing {table}")
						for tab_file in tab_files:
							# Reading data from the dat files
							tab_data = pd.DataFrame()
							for line in open(os.path.join(extractPath, tab_file), encoding='UTF-16 LE'):
								row = StringIO(line.replace("\t", " ").replace("\n", " ").replace("<EOFD>", "\t").replace("<EORD>", "\n").replace("\"", " "))
								#
								# Write logic to validate the table format
								# Check if any custom attributes are present in the data
								#
								data = pd.read_csv(row, sep="\t", header=None, names=format['fieldName']).replace('"','', regex=True)
								tab_data = tab_data.append(data)
					
							if tab_data.empty:
								print("Empty table Data")
								continue
							
							#fetching Database columns
							df = spark.sql(f"""SELECT * FROM raw_{table} WHERE 1 = 2""")
							cols = df.columns

							#Adding missing columns to the dataframe
							i = 0
							for col in cols:
								if (col not in list(format['fieldName'])) & (col != 'EXTRACT_DATE'):
									tab_data.insert(loc=i, column=col, value=None)
								i = i + 1

							viewName = f'{tab_file.split(".")[0].replace(" ", "_").replace("-", "_")}'
							tab_data = spark.createDataFrame(tab_data)
							tab_data.createOrReplaceTempView(viewName)

							#Checking whether the Full load is needed or Incremental
							if exportType[0] == "FULL":
								spark.sql(f"""
                                    INSERT OVERWRITE raw_{table}
                                    SELECT *, current_date as EXTRACT_DATE
									FROM {viewName}
                                """)
							else:
								spark.sql(f"""
									INSERT INTO raw_{table}
									SELECT *, current_date as EXTRACT_DATE
									FROM {viewName}
								""")

					break
	except Exception as err:
		print(f"Error: {err}")
		raise
	