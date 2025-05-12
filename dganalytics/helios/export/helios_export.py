from azure.storage.blob import BlobServiceClient, BlobClient, ContentSettings
from io import BytesIO
from dganalytics.utils.utils import get_secret, get_env
from dganalytics.helios.helios_utils import helios_utils_logger
import os

def helios_export(spark, tenant, extract_name, output_file_name):
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

        df = spark.sql(f"""
                select * from dgdm_{tenant}.helios_process_map where TRIM(category) NOT IN (
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
            Menu: Menu CollectCVVNumber ValidCVV','Menu: Menu CollectCVVNumber ValidCVVEn','Menu: Menu CollectCVVNumber ValidCVVEnt','Menu: Menu CollectCVVNumber ValidCVVEnte','Menu: Menu CollectCVVNumber ValidCVVEntere','Menu: Menu GenSerEnqIncludingNonActiveCase All other enquiries - Gen Enquiries Non Acti','Menu: Menu Get','Menu: Menu GetSave','Menu: Menu GetSaved','Menu: Menu GetSavedPa','Menu: Menu GetSavedPaymentMethod NewCre','Menu: Menu GetSavedPaymentMethod UseSav','Menu: Menu GetSavedPaymentMethod UseSavedPaymentMeth','Menu: Menu M','Menu: Menu Make','Menu: Menu Make Pa','Menu: Menu Make Pay','Menu: Menu MakeAnotherPaym','Menu: Menu MakeAnotherPayment1','Menu: Menu MakeAnotherPayment1 Anoth','Menu: Menu MakeAnotherPayment1 AnotherPaymentF','Menu: Menu MakeAnotherPayment1 AnotherPaymentFor','Menu: Menu MakeAnotherPayment1 AnotherPaymentForA','Menu: Menu MakeAnotherPayment1 AnotherPaymentForAnotherAc','Menu: Menu MakeAnotherPayment1 AnotherPaymentForAnotherAccoun','Menu: Bill_Co','
Menu: Bill_Copy_Pref','
Menu: Bill_Copy_Prefe','
Menu: Bill_Copy_Preference_Email_','
Menu: Bill_Copy_Preference_Email_Send_','
Menu: Bill_Copy_Preference_Email_Send_Bil','
Menu: MainMenu_','
Menu: MainMenu_Bu','
Menu: MainMenu_Bus','
Menu: MainMenu_General_Service_Enquiri','
Menu: MainMenu_MyEngieA','
Menu: MainMenu_MyEngieAp','
Menu: MainMenu_MyEngieApp_Payment_PaymentExten','
Menu: MainMenu_R','
Menu: MainMenu_Res','
Menu: MainMenu_Residenti','
Menu: MainMenu_Residential','
Menu: MainMenu_Residential IVR_Re','
Menu: Make_','
Menu: Make_Paymen','
Menu: Make_Payment_PP_','
Menu: Menu_','
Menu: Menu_A','
Menu: Menu_Acc','
Menu: Menu_Account Balan','
Menu: Menu_Account Balance_Accou','
Menu: Menu_Account Balance_Account Balance and D','
Menu: Menu_Account_','
Menu: Menu_Account_Bala','
Menu: Menu_Account_Balan','
Menu: Menu_Account_Balance_A','
Menu: Menu_Account_Balance_And','
Menu: Menu_Account_Balance_And_','
Menu: Menu_Account_Balance_And_D','
Menu: Menu_Account_Balance_And_DueDa','
Menu: Menu_Account_Balance_Re','
Menu: Menu_BusIVR-PayMenu_Acco','
Menu: Menu_C','
Menu: Menu_Co','
Menu: Menu_Col','
Menu: Menu_Coll','
Menu: Menu_Colle','
Menu: Menu_Collec','
Menu: Menu_CollectC','
Menu: Menu_CollectCr','
Menu: Menu_CollectCre','
Menu: Menu_CollectCred','
Menu: Menu_CollectCredi','
Menu: Menu_CollectCredit','
Menu: Menu_CollectCreditC','
Menu: Menu_CollectCreditCa','
Menu: Menu_CollectCreditCar','
Menu: Menu_CollectCreditCardE','
Menu: Menu_CollectCreditCardEx','
Menu: Menu_CollectCreditCardExpi','
Menu: Menu_CollectCreditCardExpir','
Menu: Menu_CollectCreditCardExpiryD','
Menu: Menu_CollectCreditCardExpiryDate&Mo','
Menu: Menu_CollectCreditCardExpiryDate&Mon','
Menu: Menu_CollectCreditCardExpiryDate&Mont','
Menu: Menu_CollectCreditCardExpiryDate&Month_','
Menu: Menu_CollectCreditCardExpiryDate&Month_E','
Menu: Menu_CollectCreditCardExpiryDate&Month_Exp','
Menu: Menu_CollectCreditCardExpiryDate&Month_Expi','
Menu: Menu_CollectCreditCardExpiryDate&Month_Expir','
Menu: Menu_CollectCreditCardExpiryDate&Month_Expiry','
Menu: Menu_CollectCreditCardExpiryDate&Month_ExpiryD','
Menu: Menu_CollectCreditCardExpiryDate&Month_ExpiryDat','
Menu: Menu_CollectCreditCardExpiryDate&Month_ExpiryDateVe','
Menu: Menu_CollectCreditCardExpiryDate&Month_ExpiryDateVer','
Menu: Menu_CollectCreditCardExpiryDate&Month_ExpiryDateVeri','
Menu: Menu_CollectCreditCardExpiryDate&Month_ExpiryDateVerifie','
Menu: Menu_CollectCreditCardExpiryDate&Month_Inva','
Menu: Menu_CollectCreditCardExpiryDate&Month_InvalidE','
Menu: Menu_CollectCreditCardExpiryDate&Month_Menu_Coll','
Menu: Menu_CollectCreditCardExpiryDate&Month_Menu_CollectCreditCardExpiryDat','
Menu: Menu_CollectCreditCardExpiryDate&Month_Menu_CollectCreditCardExpiryDate&Month_ExpiryDateV','
Menu: Menu_CollectCreditCardExpiryDate&Month_Menu_CollectCreditCardExpiryDate&Month_ExpiryDateVerifyDefau','
Menu: Menu_CollectCreditCardExpiryDate&Month_V','
Menu: Menu_CollectCreditCardExpiryDate&Month_Va','
Menu: Menu_CollectCreditCardExpiryDate&Month_ValidEx','
Menu: Menu_CollectCreditCardExpiryDate&Month_ValidExpiryD','
Menu: Menu_CollectCreditCardExpiryDate&Month_ValidExpiryDat','
Menu: Menu_CollectCreditCardExpiryDate&Month_ValidExpiryDateEnt','
Menu: Menu_CollectCreditCardExpiryDate&Month_ValidExpiryDateEnter','
Menu: Menu_CollectCreditCardN','
Menu: Menu_CollectCreditCardNu','
Menu: Menu_CollectCreditCardNumber_CCNumb','
Menu: Menu_CollectCreditCardNumber_CCNumbe','
Menu: Menu_CollectCreditCardNumber_CCNumberN'
        )  
        """)
        # df.display()
        df = df.toPandas()
        
        csv_content = df.to_csv(index=False).encode('utf-8')
        
        # Overwrite the data in the existing blob
        blob_client.upload_blob(BytesIO(csv_content), blob_type="BlockBlob", content_settings=ContentSettings(content_type="text/csv"),  overwrite=True)
    
    except Exception as e:
        logger.exception(f"An error occurred  in exporting {extract_name}: {e}")