from pyspark.sql import SparkSession


def dim_evaluations(spark: SparkSession, extract_date, extract_start_time, extract_end_time):
    pre_evaluations = spark.sql(f"""
                                    select distinct id as evaluationId, evaluator.id as evaluatorId,
                                    agent.id as agentId, conversation.id as conversationId,
                                        evaluationForm.id as evaluationFormId, status, assignedDate, releaseDate,
                                        changedDate, conversationDate, mediaType[0] as mediaType,
                                        agentHasRead, answers.anyFailedKillQuestions, answers.comments,
                                        evaluationForm.name as evaluationFormName,
                                        evaluationForm.published as evaluationFormPublished, neverRelease,
                                        resourceType,
                                        date_format(conversationDate, 'yyyy-MM-dd') conversationDatePart,recordIdentifier as sourceRecordIdentifier,
                                        concat(extractDate, '|', extractIntervalStartTime, '|', extractIntervalEndTime) as soucePartition
                                    from raw_evaluations  where extractDate = '{extract_date}'
                                    and  extractIntervalStartTime = '{extract_start_time}' and extractIntervalEndTime = '{extract_end_time}'
                                    """)
    pre_evaluations.createOrReplaceTempView("pre_evaluations")
    pre_evaluations.show()
    evaluations = spark.sql("""
                select * from (
                        select  
                            e.evaluationId, e.evaluatorId, e.agentId, e.conversationId, e.evaluationFormId, e.status,
                                e.assignedDate, e.releaseDate, e.changedDate, e.conversationDate, e.mediaType, 
                                e.agentHasRead, e.anyFailedKillQuestions, e.comments,
                                e.evaluationFormName, e.evaluationFormPublished, e.neverRelease, 
                                e.resourceType, dc.queueId, dc.wrapUpCode, e.conversationDatePart, e.sourceRecordIdentifier, e.soucePartition,
                                row_number() over(partition by dc.conversationId order by dc.sessionEnd desc) as rn
                         from pre_evaluations e, dim_conversations dc 
                            where e.conversationDatePart = dc.conversationStartDate
                            and e.conversationId = dc.conversationId
                            ) where rn = 1
            """)
    evaluations.show()
    evaluations = evaluations.drop("rn")
    
    evaluations.createOrReplaceTempView("evaluations")
    evaluations.show()
    evaluations = spark.sql("""
                                merge into dim_evaluations as target
                                    using evaluations as source
                                    on source.evaluationId = target.evaluationId
                                    and source.conversationDatePart = target.conversationDatePart
                                WHEN MATCHED THEN
                                    UPDATE SET *
                                WHEN NOT MATCHED THEN
                                    INSERT *
                            """)
