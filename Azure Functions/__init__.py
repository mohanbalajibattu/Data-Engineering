import logging
import azure.functions as func
import io
import pandas as pd
from azure.storage.blob import BlobServiceClient

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    connection_string = 'DefaultEndpointsProtocol=https;AccountName=dataresgen2;AccountKey=JamR3xU5HrMJZFYLotGNQy2VIcI986dQDhSKHl3pcsUxrfrdJae3eSaUNHpeBWv9DZwbdJg/ZBgG+ASthrco6w==;EndpointSuffix=core.windows.net'
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_name = 'read'
    blob_name = 'imdb_clean.csv'

    # Reading from ADLS Gen2
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    # Download the blob data as a stream
    blob_data = blob_client.download_blob()
    # Create an in-memory file-like object
    in_memory_file = io.BytesIO()
    blob_data.download_to_stream(in_memory_file)
    in_memory_file.seek(0)

    # writing to adls gen2
    blob_client_upload = blob_service_client.get_blob_client(container='write', blob='write.csv')
    # Upload the processed file back to Azure Blob Storage
    blob_client_upload.upload_blob(in_memory_file, overwrite=True)

    return func.HttpResponse("File Uploaded to ADLS Gen2",status_code=200)
