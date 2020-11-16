from pyspark.sql import SparkSession


def dim_questions(spark: SparkSession, extract_date, extract_start_time, extract_end_time, tenant):
    surveys = spark.sql("""
                    insert overwrite dim_questions
                        select id as questionId, question_label as questionLabel,
                            question_prompt.text.default as questionText, compulsory, disabled,
                            max_response as maxResponse,
                            string(response_options.label) labels,
                            scale_label_max_prompt.text.default scaleMaxPompt,
                            scale_label_mid_prompt.text.default scaleMidPompt,
                            scale_label_min_prompt.text.default scaleMinPompt
                        from raw_questions
                    """)
