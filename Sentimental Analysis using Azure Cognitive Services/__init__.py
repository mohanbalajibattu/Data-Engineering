import logging
import re
import azure.functions as func
import io
import pandas as pd
from azure.storage.blob import BlobServiceClient
from azure.ai.textanalytics import TextAnalyticsClient
from azure.core.credentials import AzureKeyCredential

def remove_links_and_emojis(text):
    link_pattern = re.compile(r"(https?://\S+)")
    emoji_pattern = re.compile("[\U00010000-\U0010ffff]", flags=re.UNICODE)

    # Remove links
    cleaned_text = re.sub(link_pattern, "", text)

    # Remove emojis
    cleaned_text = re.sub(emoji_pattern, "", cleaned_text)
    cleaned_text = re.sub(r'@\S+\s?', '', cleaned_text)
    return cleaned_text

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    print('In azure function')
    # name = req.params.get('name')
    # Azure Blob Storage connection string
    connection_string = '<storage-account-connection-string>'
    # Create a BlobServiceClient object
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    # Container and blob information
    container_name = 'twitter-data'
    blob_name = 'elon_musk_tweets.csv'
    # Get a BlobClient object
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

    # Download the blob data as a stream
    blob_data = blob_client.download_blob()

    # Create an in-memory file-like object
    in_memory_file = io.BytesIO()
    blob_data.download_to_stream(in_memory_file)
    in_memory_file.seek(0)

    # Load the data into a pandas DataFrame
    df = pd.read_csv(in_memory_file)
    # print(df['retweets','text','favorites'])
    # Select specific columns
    selected_columns = ['text','retweets','favorites']
    # Create a new DataFrame with the selected columns
    df_selected = df[selected_columns].copy()
    df_selected=df_selected.head(1000)
    df_selected['cleaned_text'] = df['text'].apply(remove_links_and_emojis)
    # print(df_selected['cleaned_text'])
    # Process the DataFrame as needed
    # For example, you can apply transformations or calculations
    # Set up the authentication and create a client
    endpoint = "https://sentimentanalysismb.cognitiveservices.azure.com/"
    key = "<key>"
    credential = AzureKeyCredential(key)
    client = TextAnalyticsClient(endpoint=endpoint, credential=credential)
    # print(df_selected['cleaned_text'])
    # print(ls)
    # Perform sentiment analysis on the 'cleaned_text' column
    
    # Prepare the documents for sentiment analysis
    documents = [{"id": idx, "text": text} for idx, text in enumerate(df_selected['cleaned_text']) if pd.notna(text) and text.strip()]

    # Split the documents into batches of 10 records
    batch_size = 10
    document_batches = [documents[i:i + batch_size] for i in range(0, len(documents), batch_size)]

    # Perform sentiment analysis on each batch of documents
    # Initialize lists to store the sentiment analysis scores
    sentiment_scores = [None] * len(df_selected)
    positive_scores = [None] * len(df_selected)
    negative_scores = [None] * len(df_selected)
    neutral_scores = [None] * len(df_selected)

    # Perform sentiment analysis on each batch of documents
    for batch in document_batches:
        result = client.analyze_sentiment(documents=batch)
        for doc in result:
            idx = int(doc.id)
            sentiment_scores[idx] = doc.sentiment if not doc.is_error else None
            positive_scores[idx] = doc.confidence_scores.positive if not doc.is_error else None
            negative_scores[idx] = doc.confidence_scores.negative if not doc.is_error else None
            neutral_scores[idx] = doc.confidence_scores.neutral if not doc.is_error else None

    # Add the sentiment analysis scores as new columns in the DataFrame
    df_selected['sentiment_score'] = sentiment_scores
    df_selected['positive_score'] = positive_scores
    df_selected['negative_score'] = negative_scores
    df_selected['neutral_score'] = neutral_scores
    # print(df_selected)
    # Perform sentiment analysis on the text column
    # result = client.analyze_sentiment(df_selected['cleaned_text'])
    # print(result)
    # positive_score=[]
    # negative_score=[]
    # neutral_score=[]
    # sentiment=[]
    # print('nnn')
    # for idx, doc in enumerate(result):
    #     sentiment.append(doc.sentiment)
    #     positive_score.append(doc.confidence_scores.positive)
    #     negative_score.append(doc.confidence_scores.negative)
    #     neutral_score.append(doc.confidence_scores.neutral)
    # # Add the sentiment analysis scores as a new column in the DataFrame
    # print('aaaa')
    # df_selected['sentiment_score'] =positive_score
    # print(df_selected)
    # Convert the processed DataFrame back to a file-like object
    blob_client_upload = blob_service_client.get_blob_client(container=container_name, blob='azfn-output/processed-data.csv')
    output_file = io.BytesIO()
    df_selected.to_csv(output_file, index=False)
    output_file.seek(0)

    # Upload the processed file back to Azure Blob Storage
    blob_client_upload.upload_blob(output_file, overwrite=True)
    print("file uploaded")
    return func.HttpResponse("File uploaded :)",status_code=200)
    # # Close the blob data stream
    # blob_data.close()
    # if not name:
    #     try:
    #         req_body = req.get_json()
    #     except ValueError:
    #         pass
    #     else:
    #         name = req_body.get('name')

    # if name:
    #     return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    # else:
    #     return func.HttpResponse(
    #          "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
    #          status_code=200
    #     )
