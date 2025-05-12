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
    evaluations = spark.sql(f"""
            select distinct * from (
                    select  
                        e.evaluationId, e.evaluatorId, e.agentId, e.conversationId, e.evaluationFormId, e.status,
                            e.assignedDate, e.releaseDate, e.changedDate, e.conversationDate, e.mediaType, 
                            e.agentHasRead, e.anyFailedKillQuestions, e.comments,
                            e.evaluationFormName, e.evaluationFormPublished, e.neverRelease, 
                            e.resourceType, dc.queueId, dc.wrapUpCode, e.conversationDatePart, e.sourceRecordIdentifier, e.soucePartition
                        from pre_evaluations e
                      left JOIN (SELECT DISTINCT conversationId,
                          queueId,
                          wrapUpCode,
                          conversationStartDate
                          FROM
          (SELECT conversationId,
                          co.queueId,
                          wrapUpCode,
                          conversationStartDate,
                          row_number() OVER(PARTITION BY co.conversationId ORDER BY co.sessionEnd DESC) rn
              FROM dim_conversations co
              WHERE conversationStartDate between date_sub('{extract_date}', 90) and '{extract_date}'
              AND co.wrapUpCode IS NOT NULL)
          WHERE rn = 1
          UNION ALL
          SELECT conversationId,
                  queueId,
                  wrapUpCode,
                  conversationStartDate FROM
          (SELECT DISTINCT conversationId,
                  co.queueId,
                  wrapUpCode,
                  conversationStartDate,
                  row_number() OVER (PARTITION BY co.conversationId ORDER BY co.sessionEnd DESC) rn
              FROM dim_conversations co
              WHERE cast(conversationStartDate as DATE) between date_sub('{extract_date}', 90) and '{extract_date}'
              and 
              co.conversationId in (SELECT conversationId FROM dim_conversations
              WHERE conversationStartDate  between date_sub('{extract_date}', 90) and '{extract_date}'
              
              GROUP BY conversationId
              having count(wrapUpCode) = 0))) dc 
                        ON  cast(e.conversationDatePart as date) = cast(dc.conversationStartDate as Date)
                        and 
                       e.conversationId = dc.conversationId
                        )
             
        """)
    
    evaluations.createOrReplaceTempView("evaluations")
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
