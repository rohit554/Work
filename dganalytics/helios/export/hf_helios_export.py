from azure.storage.blob import BlobServiceClient, BlobClient, ContentSettings
from io import BytesIO
from dganalytics.utils.utils import get_secret, get_env
from dganalytics.helios.helios_utils import helios_utils_logger
import os


def hf_helios_export(spark, tenant, extract_name, output_file_name):
    logger = helios_utils_logger(tenant, "helios")
    account_name = get_secret("storageaccountnameforprocessmap")
    account_key = get_secret("storageaccountkeyforprocessmap")
    try:
        # Create a BlobServiceClient
        blob_service_client = BlobServiceClient(
            account_url=f"https://{account_name}.blob.core.windows.net",
            credential=account_key,
        )

        # Get a reference to the container
        container_client = blob_service_client.get_container_client(tenant)

        # Get a reference to the existing blob
        blob_client = container_client.get_blob_client(
            os.path.join(get_env(), output_file_name)
        )

        df = spark.sql(
            f"""
                SELECT conversationid,
                category,
                action,
                action_label,
                contact_reason,
                main_inquiry,
                root_cause,
                ( CASE
                    WHEN starttime IS NULL THEN endtime
                    ELSE starttime
                     END ) startTime,
                endtime,
                speaker,
                start_line,
                end_line,
            conversationstartdateid,
            confidence,
            contribution,
            impact,
            impactreason,
            emotion,
            difficulty,
            sentiment
            FROM   dgdm_hellofresh.fact_transcript_actions
            WHERE  conversationstartdateid >= 20240901; 
        """
        )
        # df.display()
        df = df.toPandas()

        csv_content = df.to_csv(index=False).encode("utf-8")

        # Overwrite the data in the existing blob
        blob_client.upload_blob(
            BytesIO(csv_content),
            blob_type="BlockBlob",
            content_settings=ContentSettings(content_type="text/csv"),
            overwrite=True,
        )

    except Exception as e:
        logger.exception(f"An error occurred  in exporting {extract_name}: {e}")