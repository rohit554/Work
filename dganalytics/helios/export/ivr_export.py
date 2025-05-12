from azure.storage.blob import BlobServiceClient, BlobClient, ContentSettings
from io import BytesIO
from dganalytics.utils.utils import get_secret, get_env
from dganalytics.helios.helios_utils import helios_utils_logger
import os

def ivr_export(spark, tenant, extract_name, output_file_name):
    logger = helios_utils_logger(tenant,"helios")
    account_name = get_secret("storageaccountnameforprocessmap")
    account_key = get_secret("storageaccountkeyforprocessmap")
    try:
        # Create a BlobServiceClient
        blob_service_client = BlobServiceClient(account_url=f"https://{account_name}.blob.core.windows.net", credential=account_key)

        # Get a reference to the container
        container_client = blob_service_client.get_container_client(tenant)

        # Get a reference to the existing blob
        blob_client = container_client.get_blob_client(os.path.join(get_env(),output_file_name))
         
        df=spark.sql(f"""
                with conversations as (
                        select * from dgdm_{tenant}.dim_conversations
                        WHERE conversationStartDateId > 20231231 and originatingDirectionId = 1 and initialSessionMediaTypeId = 1
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
                                conversationStart as eventStart,--add 1ms
                                TIMESTAMPADD(MICROSECOND, 1000, conversationStart) as eventEnd,--add ms
                                'Start'  as eventName,
                                1 as level1,
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
                                from dgdm_simplyenergy.dim_conversation_ivr_events  as 
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
                                from (select * from dgdm_simplyenergy.dim_conversation_ivr_menu_selections order by menuEntryTime,menuExitTime) e
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
                                dgdm_simplyenergy.dim_conversation_session_segments s
                                join
                                dgdm_simplyenergy.dim_queues q
                                    on s.queueId = q.queueId
                                join dgdm_simplyenergy.dim_conversation_participants p
                                    on s.conversationStartDateId = p.conversationStartDateId
                                    and s.conversationId = p.conversationId
                                    and s.participantId = p.participantId
                                join conversations c
                                    on
                                    s.conversationStartDateId = c.conversationStartDateId and
                                    s.conversationId = c.conversationId
                                where s.queueId is not null and p.purpose not in ('customer')
                                group by s.conversationId, queueName, s.conversationStartDateId

                            UNION ALL

                            select p.conversationId,
                                min(segmentStart) as eventStart,
                                max(segmentEnd)as eventEnd,
                                'Agent ' || ROW_NUMBER() OVER (PARTITION BY p.conversationId ORDER BY MIN(segmentStart)) AS eventName,
                                200 as level1,
                                'agent' eventType,
                                p.conversationStartDateId
                            from dgdm_simplyenergy.dim_conversation_participants p
                            join dgdm_simplyenergy.dim_conversation_session_segments s 
                                on p.participantId=s.participantId 
                                and p.conversationId=s.conversationId
                            join conversations c
                                on
                                p.conversationStartDateId = c.conversationStartDateId and
                                p.conversationId = c.conversationId
                            where p.userId is not null 
                            group by p.conversationId,userId, p.conversationStartDateId

                        )
                    )


                    SELECT * FROM
                    (SELECT a.conversationId,
                        a.category,
                        a.eventStart,
                        a.eventEnd,
                        a.location,
                        a.originatingDirectionId,
                        a.mediatypeId,
                        ie.AuthenticationStatus,
                        f.conversationId IS NOT NULL isIVRSuccess,
                        fd.conversationId IS NOT NULL isIVRAbandon,
                        fe.conversationId IS NOT NULL isIVRError,
                        fq.conversationId IS NOT NULL hasIVRToQueueTransfer,
                        U.userNames,
                        UT.teamNames
                    FROM
                    (
                        SELECT c.conversationId,
                            (CASE WHEN eventType = 'menu' THEN 'Menu: ' || replace(eventName, '_', ' ') ELSE eventName END) as category,
                            eventStart,
                            case when eventEnd is null and lead(eventStart) over (PARTITION BY c.conversationId order by eventStart) is null then eventStart 
                                when eventEnd is null and lead(eventStart) over (PARTITION BY c.conversationId order by eventStart) is not null then lead(eventStart) over (PARTITION BY c.conversationId order by eventStart) 
                                else eventEnd 
                            end as eventEnd,
                            dc.location,
                            dc.originatingDirectionId,
                            dc.initialSessionMediaTypeId mediatypeId
                        FROM CTE c
                        JOIN conversations dc
                            ON dc.conversationStartDateId = c.conversationStartDateId 
                            and dc.conversationId = c.conversationId
                        GROUP BY c.conversationId,
                                eventType,
                                eventName,
                                eventStart,
                                eventEnd,
                                location,
                                originatingDirectionId,
                                initialSessionMediaTypeId


                    ) a
                    LEFT JOIN
                    (SELECT eventValue AuthenticationStatus, conversationId FROM dgdm_simplyenergy.dim_conversation_ivr_events 
                        where eventName = 'AuthenticationStatus' ) ie
                    ON  a.conversationId = ie.conversationId
                    LEFT JOIN dgdm_simplyenergy.dim_conversation_session_flow f
                    on f.conversationId = a.conversationId and f.exitReason = 'FLOW_DISCONNECT'
                    LEFT JOIN dgdm_simplyenergy.dim_conversation_session_flow fd
                    on fd.conversationId = a.conversationId and fd.exitReason = 'DISCONNECTED'
                    LEFT JOIN dgdm_simplyenergy.dim_conversation_session_flow fe
                    on fe.conversationId = a.conversationId and fe.exitReason = 'FLOW_ERROR_DISCONNECT'
                    LEFT JOIN dgdm_simplyenergy.dim_conversation_session_flow fq
                    on fq.conversationId = a.conversationId and fq.exitReason = 'TRANSFER' and fq.transferType in ('ACD', 'ACD_VOICEMAIL')
                    
                    LEFT JOIN (
                        SELECT p.conversationId, concat_ws(',', collect_list(distinct u.userFullName)) AS userNames FROM dgdm_simplyenergy.dim_conversation_participants p
                            JOIN dgdm_simplyenergy.dim_users u
                            ON u.userId = p.userId
                            JOIN dgdm_simplyenergy.dim_conversation_session_segments s
                                ON s.conversationStartDateId = p.conversationStartDateId
                                and s.conversationId = p.conversationId
                            join dgdm_simplyenergy.dim_queues q
                                on s.queueId = q.queueId
                            where p.userId IS NOT NULL and 1 = 1
                            GrOUP BY p.conversationId
                    ) U
                    ON u.conversationId = a.conversationId
                    LEFT JOIN (
                        SELECT p.conversationId, concat_ws(',', collect_list(distinct ut.TeamName)) AS teamNames FROM dgdm_simplyenergy.dim_conversation_participants p
                        JOIN dgdm_simplyenergy.dim_user_teams ut
                        ON ut.userId = p.userId
                        JOIN dgdm_simplyenergy.dim_conversation_session_segments s
                            ON s.conversationStartDateId = p.conversationStartDateId
                            and s.conversationId = p.conversationId
                        join dgdm_simplyenergy.dim_queues q
                            on s.queueId = q.queueId
                        where p.userId IS NOT NULL and 1 = 1
                        GrOUP BY p.conversationId
                    ) UT
                    ON UT.conversationId = a.conversationId
                    GROUP BY a.conversationId,
                    f.conversationId,
                    fd.conversationId,
                    fe.conversationId,
                    fq.conversationId,
                        a.category,
                        a.eventStart,
                        a.eventEnd,
                        a.location,
                        a.originatingDirectionId,
                        a.mediatypeId,
                        ie.AuthenticationStatus,
                        U.userNames,
                        UT.teamNames
                        HAVING eventEnd is NOT NULL)
                    
                    ORDER BY conversationId, eventStart, eventEnd          
        """)
        df.createOrReplaceTempView("ivr_process_map") 
        # df.display()
        spark.sql(f"""
                  insert overwrite dgdm_simplyenergy.ivr_process_map 
                  select * from ivr_process_map
                  """)
        df=spark.sql(f"""
            SELECT *  
            FROM dgdm_simplyenergy.ivr_process_map 
            WHERE TRIM(category) NOT IN (
                'Menu: M',
                'Menu: MainMe',
                'Menu: MainMenu Bu',
                'Menu: MainMenu MyEngieA',
                'Menu: MainMenu R',
                'Menu: Make Paymen',
                'Menu: Me',
                'Menu: Men',
                'Menu: Menu',
                'Menu: Menu ',
                'Menu: Menu A',
                'Menu: Menu Acc',
                'Menu: Menu Account Bala',
                'Menu: Menu Account Balance Account Balance and D',
                'Menu: Menu Account Balance And ',
                'Menu: Menu Account Balance Re',
                'Menu: Menu BusIVR-PayMenu Acco',
                'Menu: Menu C',
                'Menu: Menu Co',
                'Menu: Menu Col',
                'Menu: Menu Coll',
                'Menu: Menu Colle',
                'Menu: Menu Collec',
                'Menu: Menu Collect',
                'Menu: Menu CollectC',
                'Menu: Menu CollectCr',
                'Menu: Menu CollectCred',
                'Menu: Menu CollectCreditC',
                'Menu: Menu CollectCreditCardExpi',
                'Menu: Menu CollectCreditCardExpir',
                'Menu: Menu CollectCreditCardExpiryDate&Mon',
                'Menu: Menu CollectCreditCardExpiryDate&Month',
                'Menu: Menu CollectCreditCardExpiryDate&Month E',
                'Menu: Menu CollectCreditCardExpiryDate&Month Expi',
                'Menu: Menu CollectCreditCardExpiryDate&Month ExpiryD',
                'Menu: Menu CollectCreditCardExpiryDate&Month InvalidE',
                'Menu: Menu CollectCreditCardExpiryDate&Month Menu CollectCreditCardExpiryDat',
                'Menu: Menu CollectCreditCardExpiryDate&Month V',
                'Menu: Menu CollectCreditCardExpiryDate&Month ValidExpiryDateEnt',
                'Menu: Menu CollectCreditCardN',
                'Menu: Menu CollectCreditCardNu',
                'Menu: Menu CollectCreditCardNumber CCNumb',
                'Menu: Menu CollectCreditCardNumber CCNumbe',
                'Menu: Menu CollectCreditCardNumber CCNumberV',
                'Menu: Menu CollectCreditCardNumber CCNumberVerifi',
                'Menu: Menu CollectCVVNum',
                'Menu: Menu CollectCVVNumb',
                'Menu: Menu CollectCVVNumbe',
                'Menu: Menu CollectCVVNumber CVVNotVe',
                'Menu: Menu CollectCVVNumber CVVVerif',
                'Menu: Menu CollectCVVNumber Va',
                'Menu: Menu CollectCVVNumber Vali',
                'Menu: Menu CollectCVVNumber ValidCV',
                'Menu: Menu CollectCVVNumber ValidCVVE',
                'Menu: Menu CollectCVVNumber ValidCVVEnter',
                'Menu: Menu Ge',
                'Menu: Menu GetSa',
                'Menu: Menu GetSavedPaym',
                'Menu: Menu GetSavedPayme',
                'Menu: Menu GetSavedPaymentMethod UseSavedPay',
                'Menu: Menu Make Paymen',
                'Menu: Menu Make Payment Pay',
                'Menu: Menu MakeAnotherPayment1 Accou',
                'Menu: Menu MakeAnotherPayment1 Another',
                'Menu: Menu MakeAnotherPayment1 AnotherPaymentForAnotherAcc',
                'Menu: Menu MakeAnotherPayment1 Ret',
                'Menu: Menu MakeAnotherPayment1 Retu',
                'Menu: Menu MakeAnotherPayment1 Retur',
                'Menu: Menu MakeAnotherPayment1 ReturnToMainMe',
                'Menu: Menu MakePay-PaypalBi',
                'Menu: Menu MakePay-PaypalInitial Make Payment throu',
                'Menu: Menu P',
                'Menu: Pay',
                'Menu: Paym',
                'Menu: Payme',
                'Menu: Paymen',
                'Menu: Payment Acco',
                'Menu: Payment Account Balanc',
                'Menu: PaymentCo',
                'Menu: PaymentConfi',
                'Menu: PaymentConfir',
                'Menu: PaymentConfirm',
                'Menu: PaymentConfirma',
                'Menu: PaymentConfirmat',
                'Menu: PaymentConfirmation Amount&A',
                'Menu: PaymentConfirmation Amount&Ac',
                'Menu: PaymentConfirmation Amount&Accoun',
                'Menu: PaymentConfirmation Amount&Account',
                'Menu: PaymentConfirmation Amount&AccountVe',
                'Menu: PaymentConfirmation Amount&AccountVeri',
                'Menu: PaymentConfirmation Amount&AccountVerifi',
                'Menu: PaymentConfirmation Amount&AccountVerifyAmoun',
                'Menu: PaymentFailureF',
                'Menu: Payments Account B',
                'Menu: Payments Account Balance Due',
                'Menu: Menu S',
                'Menu: Menu Sa',
                'Menu: Menu Sav',
                'Menu: Menu Save',
                'Menu: Menu Save Cred',
                'Menu: Menu Save Credi',
                'Menu: Menu Save Credit Car',
                'Menu: Menu SaveCr',
                'Menu: Menu SaveCre',
                'Menu: Menu SaveCred',
                'Menu: Menu SaveCredi',
                'Menu: Menu SaveCredit',
                'Menu: Menu SaveCreditC',
                'Menu: Menu SaveCreditCard S',
                'Menu: Menu SaveCreditCard SaveCar',
                'Menu: Menu SaveCreditCard Sk',
                'Menu: Menu SaveCreditCard Ski',
                'Menu: Menu SaveCreditCard Skip',
                'Menu: Menu UpdatePayment ValidUpdatePaymentA',
                'Menu: Menu WhatsNextBill No C',
                'Menu: Non WA C',
                'Menu: P',
                'Menu: Pay',
                'Menu: Payme',
                'Menu: Paymen',
                'Menu: Payment S',
                'Menu: Payment Sta',
                'Menu: Payment Stan',
                'Menu: Payment StandA',
                'Menu: Payment StandAl',
                'Menu: Payment StandAlone P',
                'Menu: Payment StandAlone Pr',
                'Menu: Payment StandAlone Pro',
                'Menu: RouteTo AccountBal',
                'Menu: RouteTo Gener',
                'Menu: Start Pa',
                'Menu: Start Paym',
                'Menu: Start PaymentS',
                'Menu: Start PaymentSta',
                'Menu: Start PaymentStandalone Pa',
                'Menu: Bill Co','Menu: Bill Copy Pref','Menu: Bill Copy Prefe','Menu: Bill Copy Preference Email Send Bil','Menu: Ma','Menu: Bill Copy Pref','Menu: Bill Copy Prefe','Menu: MainMen','Menu: MainMenu MyEngieAp','Menu: MainMenu MyEngieApp Payment PaymentExten','Menu: MainMenu Res','Menu: MainMenu Residenti','Menu: MainMenu Residential IVR Re','Menu: MainMenu ResidentialIVR Uauth General Service Enqu','Menu: Make','Menu: Menu Account Balance And','Menu: Menu Account Balance And D','Menu: Menu Account Balance And DueDa','Menu: Menu Bill Copy Email Pref','Menu: Menu BusIVR-Pa','Menu: Menu CollectCre','Menu: Menu CollectCredi','Menu: Menu CollectCredit','Menu: Menu CollectCreditCa','Menu: Menu CollectCreditCar','Menu: Menu CollectCreditCardE','Menu: Menu CollectCreditCardEx','Menu: Menu CollectCreditCardExpiry','Menu: Menu CollectCreditCardExpiryD','Menu: Menu CollectCreditCardExpiryDate&Mo','Menu: Menu CollectCreditCardExpiryDate&Mont','Menu: Menu CollectCreditCardExpiryDate&Month Exp','Menu: Menu CollectCreditCardExpiryDate&Month Expir','Menu: Menu CollectCreditCardExpiryDate&Month ExpiryDat','Menu: Menu CollectCreditCardExpiryDate&Month ExpiryDateVe','Menu: Menu CollectCreditCardExpiryDate&Month ExpiryDateVer','
            Menu: Menu CollectCreditCardExpiryDate&Month ExpiryDateVeri','Menu: Menu CollectCreditCardExpiryDate&Month ExpiryDateVerifie','Menu: Menu CollectCreditCardExpiryDate&Month Inva','Menu: Menu CollectCreditCardExpiryDate&Month Menu Coll','Menu: Menu CollectCreditCardExpiryDate&Month Menu CollectCreditCardExpiryDate&Month ExpiryDateV','
            Menu: Menu CollectCreditCardExpiryDate&Month Menu CollectCreditCardExpiryDate&Month ExpiryDateVerifyDefau','Menu: Menu CollectCreditCardExpiryDate&Month Va','
            Menu: Menu CollectCreditCardExpiryDate&Month ValidEx','Menu: Menu CollectCreditCardExpiryDate&Month ValidExpiryD','Menu: Menu CollectCreditCardExpiryDate&Month ValidExpiryDat','Menu: Menu CollectCreditCardNumber CCNumberN','Menu: Menu CollectCreditCardNumber CCNumberVer','Menu: Menu CollectCreditCardNumber CCNumberVeri','Menu: Menu CollectCVVN','Menu: Menu CollectCVVNu','Menu: Menu CollectCVVNumber CVVVe','Menu: Menu CollectCVVNumber CVVVer',
            'Menu: Menu CollectCVVNumber CVVVeri','Menu: Menu CollectCVVNumber CVVVerifie','Menu: Menu CollectCVVNumber Invali','Menu: Menu CollectCVVNumber InvalidCVVEnte','Menu: Menu CollectCVVNumber V','Menu: Menu CollectCVVNumber Valid','Menu: Menu CollectCVVNumber ValidC','
            Menu: Menu CollectCVVNumber ValidCVV','Menu: Menu CollectCVVNumber ValidCVVEn','Menu: Menu CollectCVVNumber ValidCVVEnt','Menu: Menu CollectCVVNumber ValidCVVEnte','Menu: Menu CollectCVVNumber ValidCVVEntere','Menu: Menu GenSerEnqIncludingNonActiveCase All other enquiries - Gen Enquiries Non Acti','Menu: Menu Get','Menu: Menu GetSave','Menu: Menu GetSaved','Menu: Menu GetSavedPa','Menu: Menu GetSavedPaymentMethod NewCre','Menu: Menu GetSavedPaymentMethod UseSav','Menu: Menu GetSavedPaymentMethod UseSavedPaymentMeth','Menu: Menu M','Menu: Menu Make','Menu: Menu Make Pa','Menu: Menu Make Pay','Menu: Menu MakeAnotherPaym','Menu: Menu MakeAnotherPayment1','Menu: Menu MakeAnotherPayment1 Anoth','Menu: Menu MakeAnotherPayment1 AnotherPaymentF','Menu: Menu MakeAnotherPayment1 AnotherPaymentFor','Menu: Menu MakeAnotherPayment1 AnotherPaymentForA','Menu: Menu MakeAnotherPayment1 AnotherPaymentForAnotherAc','Menu: Menu MakeAnotherPayment1 AnotherPaymentForAnotherAccoun'
        ) 
        """)
        df = df.toPandas()
        
        csv_content = df.to_csv(index=False).encode('utf-8')
        
        # Overwrite the data in the existing blob
        blob_client.upload_blob(BytesIO(csv_content), blob_type="BlockBlob", content_settings=ContentSettings(content_type="text/csv"),  overwrite=True)
    except Exception as e:
        print(f"An error occurred  in exporting IVRPROCESSMAP:",e)