from dganalytics.utils.utils import get_spark_session, get_path_vars, export_powerbi_csv, push_gamification_data
import os
from pyspark.sql.functions import col, date_format, to_timestamp, regexp_replace, coalesce, lit
from pyspark.sql.types import DoubleType
import pandas as pd 

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_file', required=True)

    args, unknown_args = parser.parse_known_args()
    input_file = args.input_file

    tenant = 'datagamz'
    spark = get_spark_session('inbound_data', tenant)
    customer = 'skynz'
    db_name = f"dg_{customer}"
    tenant_path, db_path, log_path = get_path_vars(customer)
    
    if input_file.endswith(".json"):
        inbound = pd.read_json(os.path.join(tenant_path, 'data', 'raw', 'quality_inbound_data', input_file))

    inbound = pd.DataFrame(inbound['values'].tolist())
    header = inbound.iloc[0]
    inbound = inbound[1:]
    inbound.columns = header
    inbound['orgId'] = 'skynzib'

    inbound = inbound[['EID', 'Timestamp', 'Overall Score', 'Score: Create Connection', 'Score: Wrap','orgId']]
    inbound.rename(columns={'Overall Score': 'Overall_Score', 'Score: Create Connection': 'Score_Create_Connection', 'Score: Wrap': 'Score_Wrap'}, inplace=True)

    inbound = spark.createDataFrame(inbound)

    inbound = inbound.withColumn('Overall_Score', regexp_replace('Overall_Score', '%', ''))
    inbound = inbound.withColumn('Overall_Score', col('Overall_Score').cast(DoubleType()))
    inbound = inbound.withColumn('Overall_Score', coalesce(col('Overall_Score'), lit('')))

    inbound = inbound.withColumn('Score_Create_Connection', regexp_replace('Score_Create_Connection', '%', ''))
    inbound = inbound.withColumn('Score_Create_Connection', col('Score_Create_Connection').cast(DoubleType()))
    inbound = inbound.withColumn('Score_Create_Connection', coalesce(col('Score_Create_Connection'), lit('')))

    inbound = inbound.withColumn('Score_Wrap', regexp_replace('Score_Wrap', '%', ''))
    inbound = inbound.withColumn('Score_Wrap', col('Score_Wrap').cast(DoubleType()))
    inbound = inbound.withColumn('Score_Wrap', coalesce(col('Score_Wrap'), lit('')))

    inbound = inbound.withColumn('Timestamp', date_format(to_timestamp('Timestamp', 'M/d/yyyy H:mm:ss'), 'MM-dd-yyyy HH:mm:ss'))

    inbound.createOrReplaceTempView("inbound_data")

    spark.sql(f"""INSERT INTO {db_name}.inbound_quality_data
                    SELECT *
                    FROM inbound_data
                    WHERE Timestamp NOT IN (SELECT Timestamp FROM {db_name}.inbound_quality_data)
                    """)

    df = spark.sql("""
                      SELECT
                          b.Sky_ID as `userId`,
                          a.`Timestamp` AS `Date`,
                          a.`Overall_Score` AS `QA Score`,
                          a.`Score_Create_Connection` AS `QA Score - Create Connection`,
                          a.`Score_Wrap` AS `QA Score - Wrap`    
                      FROM
                          dg_skynz.inbound_quality_data a
                      JOIN
                          dg_skynz.user_data b
                    ON
                        a.EID = b.Probe_ID
                    WHERE
                        b.Sky_ID IS NOT NULL
                        AND length(trim(b.Sky_ID)) > 0
                    """)
    
    push_gamification_data(df.toPandas(), customer.upper(), 'SKYIB_QA_Connection')