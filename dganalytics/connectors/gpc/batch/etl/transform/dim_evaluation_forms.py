from pyspark.sql import SparkSession


def dim_evaluation_forms(spark: SparkSession, extract_date: str):
    evaluation_forms = spark.sql("""
                    insert overwrite dim_evaluation_forms
                            select distinct id as evaluationFormId, name as evaluationFormName,
                            published from raw_evaluation_forms 
                    """)