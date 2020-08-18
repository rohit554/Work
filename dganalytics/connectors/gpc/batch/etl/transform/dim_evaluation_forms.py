from dganalytics.utils.utils import get_spark_session
from dganalytics.connectors.gpc.gpc_utils import parser, get_dbname

if __name__ == "__main__":
    tenant, run_id, extract_date = parser()
    db_name = get_dbname(tenant)
    spark = get_spark_session(app_name="dim_evaluation_forms", tenant=tenant, default_db=db_name)

    evaluation_forms = spark.sql(f"""
				insert overwrite dim_evaluation_forms
                        select id as evaluationFormId, name as evaluationFormName, published from raw_evaluation_forms 
	            """)
