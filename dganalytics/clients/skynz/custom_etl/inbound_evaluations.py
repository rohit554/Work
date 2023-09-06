from dganalytics.utils.utils import get_spark_session, get_path_vars, export_powerbi_csv, push_gamification_data
import os
from pyspark.sql.functions import col, date_format, to_timestamp, regexp_replace, coalesce, lit
from pyspark.sql.types import DoubleType
import pandas as pd 
from datetime import datetime
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_file', required=True)

    args, unknown_args = parser.parse_known_args()
    input_file = args.input_file

    tenant = 'skynzib'
    db_name = f"dg_{tenant}"
    spark = get_spark_session('inbound_evaluations', tenant, default_db=db_name)
    tenant_path, db_path, log_path = get_path_vars(tenant)
    
    if input_file.endswith(".json"):
        evaluations = pd.read_json(os.path.join(tenant_path, 'data', 'raw', 'inbound_evaluations', input_file))

    else:
      raise Exception("File is not in the correct format. It should have a '.json' extension.")

    evaluations = pd.DataFrame(evaluations['values'].tolist())
    header = evaluations.iloc[0]
    evaluations = evaluations[1:]
    evaluations.columns = header
    evaluations['orgId'] = tenant

    evaluations = evaluations[['EID', 'Timestamp', 'Overall Score', 'Score: Create Connection', 'Score: Wrap','orgId']]
    evaluations.rename(columns={'Overall Score': 'Overall_Score', 'Score: Create Connection': 'Score_Create_Connection', 'Score: Wrap': 'Score_Wrap'}, inplace=True)

    evaluations = spark.createDataFrame(evaluations)

    evaluations = evaluations.withColumn('Overall_Score', regexp_replace('Overall_Score', '%', ''))
    evaluations = evaluations.withColumn('Overall_Score', col('Overall_Score').cast(DoubleType()))
    evaluations = evaluations.withColumn('Overall_Score', coalesce(col('Overall_Score'), lit('')))

    evaluations = evaluations.withColumn('Score_Create_Connection', regexp_replace('Score_Create_Connection', '%', ''))
    evaluations = evaluations.withColumn('Score_Create_Connection', col('Score_Create_Connection').cast(DoubleType()))
    evaluations = evaluations.withColumn('Score_Create_Connection', coalesce(col('Score_Create_Connection'), lit('')))

    evaluations = evaluations.withColumn('Score_Wrap', regexp_replace('Score_Wrap', '%', ''))
    evaluations = evaluations.withColumn('Score_Wrap', col('Score_Wrap').cast(DoubleType()))
    evaluations = evaluations.withColumn('Score_Wrap', coalesce(col('Score_Wrap'), lit('')))

    evaluations = evaluations.withColumn('Timestamp', date_format(to_timestamp('Timestamp', 'M/d/yyyy H:mm:ss'), 'MM-dd-yyyy HH:mm:ss'))

    evaluations.createOrReplaceTempView("inbound_evaluations")

    spark.sql(f"""MERGE into {db_name}.inbound_evaluations DB
                    USING inbound_evaluations A
                    ON A.EID = DB.EID
                    and A.Timestamp = DB.Timestamp
                    WHEN MATCHED THEN
                    UPDATE SET *
                    WHEN NOT MATCHED THEN
                    INSERT *
                    """)

    df = spark.sql(f"""
                      SELECT
                          DISTINCT
                          b.Sky_ID as `userId`,
                          a.`Timestamp` AS `Date`,
                          a.`Overall_Score` AS `QA Score`,
                          a.`Score_Create_Connection` AS `QA Score - Create Connection`,
                          a.`Score_Wrap` AS `QA Score - Wrap`    
                      FROM
                          {db_name}.inbound_evaluations a
                      JOIN
                          {db_name}.users b
                    ON
                        a.EID = b.Probe_ID
                    WHERE
                        b.Sky_ID IS NOT NULL
                        AND length(trim(b.Sky_ID)) > 0
                    """)
    
    push_gamification_data(df.toPandas(), customer.upper(), 'SKYIB_QA_Connection')