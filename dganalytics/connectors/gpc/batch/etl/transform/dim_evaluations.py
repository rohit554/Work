from pyspark.sql import SparkSession


def dim_evaluations(spark: SparkSession, extract_date, extract_start_time, extract_end_time):
    evaluations = spark.sql(f"""
                                    select distinct id as evaluationId, evaluator.id as evaluatorId,
                                    agent.id as agentId, conversation.id as conversationId,
                                        evaluationForm.id as evaluationFormId, status, assignedDate, releaseDate,
                                        changedDate, conversationDate, mediaType[0] as mediaType,
                                        agentHasRead, answers.anyFailedKillQuestions, answers.comments,
                                        evaluationForm.name as evaluationFormName,
                                        evaluationForm.published as evaluationFormPublished, neverRelease,
                                        resourceType, cast(assignedDate as date) as assignedDatePart
                                    from raw_evaluations  where extractDate = '{extract_date}'
                                    and  startTime = '{extract_start_time}' and endTime = '{extract_end_time}'
                                    """)
    evaluations.registerTempTable("evaluations")
    evaluations = spark.sql("""
                                merge into dim_evaluations as target
                                    using evaluations as source
                                    on source.evaluationId = target.evaluationId
                                WHEN MATCHED THEN
                                    UPDATE SET *
                                WHEN NOT MATCHED THEN
                                    INSERT *
                            """)
