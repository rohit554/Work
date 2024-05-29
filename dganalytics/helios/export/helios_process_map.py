from azure.storage.blob import BlobServiceClient, BlobClient, ContentSettings
from io import BytesIO
from dganalytics.utils.utils import get_secret, get_env
from dganalytics.helios.helios_utils import helios_utils_logger
import os

def helios_process_map(spark, tenant):
  logger = helios_utils_logger(tenant,"helios")
  queue_condition=''
  conversations=''
  if tenant == 'hellofresh':
      queue_condition= """q.queueName in ('Green Chef UK General Escalation', 'Green Chef UK No Recording',
                  'Green Chef UK Serious Complaints',
                  'Green Chef UK Training',
                  'UK Soft Call to Cancel',
                  'UK GDPR',
                  'UK Green Chef Data Protection',
                  'Green Chef UK',
                  'UK',
                  'GC UK - Data Deletion',
                  'HF UK - Data Deletion',
                  'UK Delivery Satisfaction',
                  'UK Seniors',
                  'UK VIP')"""
              
      conversations = f"""select distinct c.* from dgdm_{tenant}.dim_conversations c
          join dgdm_{tenant}.dim_conversation_session_segments s
          on c.conversationId = s.conversationId
          join dgdm_{tenant}.dim_queues q
          on s.queueId = q.queueId
          where q.queueName in ('Green Chef UK General Escalation', 'Green Chef UK No Recording',
                                  'Green Chef UK Serious Complaints',
                                  'Green Chef UK Training',
                                  'UK Soft Call to Cancel',
                                  'UK GDPR',
                                  'UK Green Chef Data Protection',
                                  'Green Chef UK',
                                  'UK',
                                  'GC UK - Data Deletion',
                                  'HF UK - Data Deletion',
                                  'UK Delivery Satisfaction',
                                  'UK Seniors',
                                  'UK VIP'
          ) and c.initialSessionMediaTypeId = 1
          and c.conversationStartDateId between 20231101 and 20231130
      """
  else:
      queue_condition='1=1'  
      conversations = f"""select * from dgdm_{tenant}.dim_conversations
              WHERE originatingDirectionId=1 
              and initialSessionMediaTypeId=1 
              and conversationStartDateId >= (select dateId from dgdm_{tenant}.dim_date where dateVal = DATE_SUB(CURRENT_DATE,10))     
            """
  df=spark.sql(f"""
      with conversations as (
          {conversations}
      ),
      CTE AS (
          SELECT
              conversationId, eventName,
              (case WHEN eventType = 'authStatus' THEN COALESCE(eventStart, TIMESTAMPADD(MICROSECOND, 1000, LAG(eventEnd, 1) OVER (PARTITION BY conversationId ORDER BY level1))) 
                                          ELSE eventStart END)
                                          AS eventStart,
              (case WHEN eventType = 'authStatus' THEN COALESCE(eventEnd, TIMESTAMPADD(MICROSECOND, 1000, LAG(eventEnd, 1) OVER (PARTITION BY conversationId ORDER BY level1))) 
                              ELSE eventEnd END)
                              AS eventEnd,
              level1,
              eventType,
              conversationStartDateId
          FROM (
              select conversationId,
                  conversationStart as eventStart,
                  TIMESTAMPADD(MICROSECOND, 1000, conversationStart) as eventEnd,
                  'Start' as eventName,
                  0 as level1,
                  'caller' eventType,
                  conversationStartDateId
                  FROM conversations
                  
                                    
                  UNION ALL

                  select 
                  as.conversationId,
                  null as eventStart,
                  null as eventEnd,
                  eventName || ": "|| eventValue eventName,
                  2 as level1,
                  'authStatus' eventType,
                  as.conversationStartDateId
                  from dgdm_{tenant}.dim_conversation_ivr_events  as 
                  join conversations c
                  on
                  as.conversationStartDateId = c.conversationStartDateId 
                  and as.conversationId = c.conversationId
                  where eventName='AuthenticationStatus'

              UNION ALL

              select 
                  f.conversationId,
                  cast(
                          concat(
                              from_unixtime((cast(eventTime as double) - (cast(value as double)/1000))),
                                '.',
                                FLOOR((cast(eventTime as double)*1000 - (cast(value as double)))%1000)
                              ) as timestamp
                      ) as eventStart,--add 1ms
                      eventTime as eventEnd,
                  'Flow: ' ||flowName as eventName,
                  3 as level1,
                  'flow' eventType,
                  f.conversationStartDateId
                  from dgdm_simplyenergy.dim_conversation_session_flow f
                  join
                  dgdm_simplyenergy.fact_conversation_metrics m 
                  on
                      f.conversationStartDateId = m.conversationStartDateId
                      and f.conversationId = m.conversationId
                      and f.sessionId = m.sessionId
                  join conversations c
                      on
                      f.conversationStartDateId = c.conversationStartDateId and
                      f.conversationId = c.conversationId
                    where m.name = 'tFlow'


              UNION ALL

            
                    
                select e.conversationId,
                    menuEntryTime as eventStart,
                    menuExitTime as eventEnd,
                    menuId as eventName,
                    3+index as level1,
                    'menu' eventType,
                    e.conversationStartDateId
                    from (select * from dgdm_{tenant}.dim_conversation_ivr_menu_selections order by menuEntryTime,menuExitTime) e
                    INNER JOIN conversations c
                                    ON e.conversationStartDateId = c.conversationStartDateId
                                    AND e.conversationId = c.conversationId


              UNION ALL

              select  
                  s.conversationId,
                  min(segmentStart) as eventStart,
                  max(segmentEnd) as eventEnd,
                  'Queue: ' ||q.queueName as eventName,
                  100 as level1,
                  'queue' eventType,
                  s.conversationStartDateId
                  from
                  dgdm_{tenant}.dim_conversation_session_segments s
                  join
                  dgdm_{tenant}.dim_queues q
                      on s.queueId = q.queueId
                  join dgdm_{tenant}.dim_conversation_participants p
                      on s.conversationStartDateId = p.conversationStartDateId
                      and s.conversationId = p.conversationId
                      and s.participantId = p.participantId
                  join conversations c
                      on
                      s.conversationStartDateId = c.conversationStartDateId and
                      s.conversationId = c.conversationId
                  where s.queueId is not null and p.purpose not in ('customer','external')
                  group by s.conversationId, queueName, s.conversationStartDateId

              UNION ALL

              select p.conversationId,
                  min(segmentStart) as eventStart,
                  max(segmentEnd)as eventEnd,
                  'Agent ' || ROW_NUMBER() OVER (PARTITION BY p.conversationId ORDER BY MIN(segmentStart)) AS eventName,
                  200 as level1,
                  'agent' eventType,
                  p.conversationStartDateId
              from dgdm_{tenant}.dim_conversation_participants p
              join dgdm_{tenant}.dim_conversation_session_segments s 
                  on p.participantId=s.participantId 
                  and p.conversationId=s.conversationId
              join conversations c
                  on
                  p.conversationStartDateId = c.conversationStartDateId and
                  p.conversationId = c.conversationId
              where p.userId is not null 
              group by p.conversationId,userId, p.conversationStartDateId

              UNION ALL

              select 
                  b.conversationId,
                  eventTime eventStart,
                  eventTime eventEnd,
                  'Blind Transferred' as eventName,
                  250 as level1,
                  'blindTransfer' eventType,
                  b.conversationStartDateId
              from dgdm_{tenant}.fact_conversation_metrics b
              join conversations c
                  on
                  b.conversationStartDateId = c.conversationStartDateId and
                  b.conversationId = c.conversationId
              where name='nBlindTransferred' 

              UNION ALL

              select 
                  ct.conversationId,
                  eventTime eventStart,
                  eventTime eventEnd,
                  'Consult Transferred' as eventName,
                  300 as level1, 
                  'consultTransfer' eventType,
                  ct.conversationStartDateId
              from dgdm_{tenant}.fact_conversation_metrics ct
              join conversations c
                  on
                  ct.conversationStartDateId = c.conversationStartDateId and
                  ct.conversationId = c.conversationId
              where name='nConsultTransferred'

              UNION ALL

              select distinct 
                  cm.conversationId,
                  eventTime eventStart,
                  eventTime eventEnd,
                  'Consult' as eventName,
                  350 as level1,
                  'consult' eventType,
                  cm.conversationStartDateId
              from dgdm_{tenant}.fact_conversation_metrics cm
              join conversations c
                  on
                  cm.conversationStartDateId = c.conversationStartDateId and
                  cm.conversationId = c.conversationId
              where name='nConsult' 

              UNION ALL

              select 
                  h.conversationId,
                  eventTime eventStart,
                  eventTime eventEnd,
                  'Hold' as eventName,
                  400 as level1,
                  'hold' eventType,
                  h.conversationStartDateId
              from dgdm_{tenant}.fact_conversation_metrics h
              join conversations c
                  on
                  h.conversationStartDateId = c.conversationStartDateId and
                  h.conversationId = c.conversationId
              where name='tHeld'  

          )
      )


      SELECT a.conversationId,
          a.category,
          a.action,
          a.action_label,
          a.eventStart,
          a.eventEnd,
          a.contact_reason,
          a.main_inquiry,
          a.root_cause,
          a.location,
          a.originatingDirectionId,
          a.mediatypeId,
          ie.AuthenticationStatus,
          (CASE WHEN f.resolved IS NOT NULL THEN f.resolved ELSE i.resolved END) resolved,
          MAX(CASE WHEN fc.name = 'tHeld' THEN True ELSE False END) AS hashold,
          MAX(CASE WHEN fc.name = 'nConsult' THEN True ELSE False END) AS hasconsult,
          MAX(CASE WHEN fc.name = 'nConsultTransferred' THEN True ELSE False END) AS hasconsulttransfer,
          MAX(CASE WHEN fc.name = 'nBlindTransferred' THEN True ELSE False END) AS hasblindtransfer,
          U.userNames,
          UT.teamNames,
          a.speaker,
          fqn.finalQueueName,
          fwcn.finalWrapupCode,
          a.conversationStartDateId
      FROM
      (
          SELECT c.conversationId,
              (CASE WHEN eventType = 'menu' THEN 'Menu: ' || eventName ELSE eventName END) as category,
              '' action,
              '' action_label,
              case when eventStart is null then lag(eventEnd) over (PARTITION BY c.conversationId order by eventStart)
                                when eventStart is null and lag(eventEnd) over (PARTITION BY c.conversationId order by eventStart) is null then  dc.conversationStart 
                                else eventStart 
                                END AS eventStart,
              case when eventEnd is null then lead (eventStart) over (PARTITION BY c.conversationId order by eventStart) else eventEnd end as eventEnd,
              case when FIRST(f.conversationId) IS NULL then 'Ended in IVR' else i.contactReason end as contact_reason,
              case when FIRST(f.conversationId) IS NULL  then 'Ended in IVR' else i.mainInquiry end as main_inquiry,
              case when FIRST(f.conversationId) IS NULL  then 'Ended in IVR' else i.rootCause end as root_cause,
              dc.location,
              dc.originatingDirectionId,
              dc.initialSessionMediaTypeId mediatypeId,
              '' speaker,
              c.conversationStartDateId
          FROM CTE c
          JOIN conversations dc
              ON dc.conversationStartDateId = c.conversationStartDateId 
              and dc.conversationId = c.conversationId
          LEFT JOIN dgdm_{tenant}.fact_transcript_contact_reasons i
              ON c.conversationId = i.conversationId
              AND c.conversationStartDateId = i.conversationStartDateId
          LEFT JOIN dgdm_{tenant}.dim_conversation_session_flow f
              ON f.conversationId = c.conversationId
              AND f.conversationStartDateId = c.conversationStartDateId
              AND f.exitReason = 'TRANSFER' AND f.transferType in ( 'ACD' ,'ACD_VOICEMAIL')
          GROUP BY c.conversationId,
                  eventType,
                  eventName,
                  eventStart,
                  eventEnd,
                  contactReason,
                  mainInquiry,
                  rootCause,
                  location,
                  originatingDirectionId,
                  initialSessionMediaTypeId,
                  C.conversationStartDateId,
                  dc.conversationStart

      UNION ALL

          SELECT  a.conversationId,
              category,
              action,
              action_label,
              startTime eventStart,
              endTime eventEnd,
              contact_reason,
              main_inquiry,
              root_cause,
              dc.location,
              dc.originatingDirectionId,
              dc.initialSessionMediaTypeId mediatypeId,
              a.speaker,
              dc.conversationStartDateId
          FROM dgdm_{tenant}.fact_transcript_actions a
          JOIN conversations dc
          ON 
          dc.conversationId = a.conversationId
      ) a
      LEFT JOIN  dgdm_{tenant}.fact_conversation_metrics fc
      ON fc.conversationId = a.conversationid
          AND fc.conversationStartDateId = a.conversationStartDateId
      LEFT JOIN 
      (SELECT eventValue AuthenticationStatus, conversationStartDateId, conversationId FROM dgdm_{tenant}.dim_conversation_ivr_events 
          where eventName = 'AuthenticationStatus' ) ie
      ON  a.conversationId = ie.conversationId
          AND ie.conversationStartDateId = a.conversationStartDateId
      LEFT JOIN dgdm_{tenant}.fact_transcript_insights i
      ON i.conversationId = a.conversationId
          AND i.conversationStartDateId = a.conversationStartDateId
      LEFT JOIN (SELECT case when exitReason = 'FLOW_DISCONNECT' THEN 'resolved' END as resolved, 
                      conversationId, conversationStartDateId from dgdm_{tenant}.dim_conversation_session_flow
              ) f
      on f.conversationId = a.conversationId
          AND f.conversationStartDateId = a.conversationStartDateId
      LEFT JOIN (
          SELECT p.conversationId, concat_ws(',', collect_list(distinct u.userFullName)) AS userNames, p.conversationStartDateId FROM dgdm_{tenant}.dim_conversation_participants p
              JOIN dgdm_{tenant}.dim_users u
              ON u.userId = p.userId
              JOIN dgdm_{tenant}.dim_conversation_session_segments s
                  ON s.conversationStartDateId = p.conversationStartDateId
                  and s.conversationId = p.conversationId
              join dgdm_{tenant}.dim_queues q
                  on s.queueId = q.queueId
              where p.userId IS NOT NULL and {queue_condition}
              GrOUP BY p.conversationId, p.conversationStartDateId
      ) U
      ON u.conversationId = a.conversationId
          AND u.conversationStartDateId = a.conversationStartDateId
      LEFT JOIN (
          SELECT p.conversationId, concat_ws(',', collect_list(distinct ut.TeamName)) AS teamNames, p.conversationStartDateId FROM dgdm_{tenant}.dim_conversation_participants p
          JOIN dgdm_{tenant}.dim_user_teams ut
          ON ut.userId = p.userId
          JOIN dgdm_{tenant}.dim_conversation_session_segments s
              ON s.conversationStartDateId = p.conversationStartDateId
              and s.conversationId = p.conversationId
          join dgdm_{tenant}.dim_queues q
              on s.queueId = q.queueId
          where p.userId IS NOT NULL and {queue_condition}
          GrOUP BY p.conversationId, p.conversationStartDateId
      ) UT
      ON UT.conversationId = a.conversationId
          AND UT.conversationStartDateId = a.conversationStartDateId
      LEFT JOIN (
              SELECT s.conversationId, s.conversationStartDateId, q.queueName as finalQueueName
              FROM (
                      Select conversationId,
                              queueId, 
                              conversationStartDateId,
                              ROW_NUMBER() OVER (
                                      PARTITION BY conversationStartDateId, conversationId 
                                      ORDER BY segmentEnd DESC
                                  ) AS rn
                      from dgdm_{tenant}.dim_conversation_session_segments
                  ) s
              JOIN dgdm_{tenant}.dim_queues AS q 
              ON s.queueId = q.queueId
              WHERE s.rn = 1
          ) fqn
          ON fqn.conversationId = a.conversationId
              AND fqn.conversationStartDateId = a.conversationStartDateId
          LEFT JOIN (
              SELECT s.conversationId, s.conversationStartDateId, w.wrapUpCode AS finalWrapupCode
              FROM (
                  SELECT 
                      conversationId, 
                      wrapUpCodeId,
                      conversationStartDateId,
                      ROW_NUMBER() OVER (
                          PARTITION BY conversationStartDateId, conversationId, segmentType 
                          ORDER BY segmentEnd DESC
                      ) AS rn
                  FROM dgdm_{tenant}.dim_conversation_session_segments
                  WHERE segmentType = 'wrapup'
              ) AS s
              JOIN dgdm_{tenant}.dim_wrap_up_codes AS w ON s.wrapUpCodeId = w.wrapUpId
              WHERE s.rn = 1
          ) fwcn
          ON fwcn.conversationId = a.conversationId
              AND fwcn.conversationStartDateId = a.conversationStartDateId
      GROUP BY a.conversationId,
          a.category,
          a.action,
          a.action_label,
          a.eventStart,
          a.eventEnd,
          a.contact_reason,
          a.main_inquiry,
          a.root_cause,
          a.location,
          a.originatingDirectionId,
          a.mediatypeId,
          ie.AuthenticationStatus,
          i.resolved,
          f.resolved,
          --f.exitReason,
          U.userNames,
          UT.teamNames,
          a.speaker,
          fqn.finalQueueName,
          fwcn.finalWrapupCode,
          a.conversationStartDateId
        HAVING eventEnd is NOT NULL
      ORDER BY a.conversationId, a.eventStart, a.eventEnd
  """)
  df.createOrReplaceTempView("helios_process_map") 
  spark.sql(f"""
    delete from dgdm_{tenant}.helios_process_map hpm
    where conversationStartDateId >= (select dateId from dgdm_{tenant}.dim_date where dateVal = DATE_SUB(CURRENT_DATE,10))

  """)

  spark.sql(f"""
      insert into dgdm_{tenant}.helios_process_map select * from helios_process_map      
  """)
