# gcs_upload.py
# Copyright (c) 2025 (Your Name/Organization, inspired by Stephen G. Pope's structure)
#
# This module provides functionality to stream files from a URL
# directly to Google Cloud Storage.

import os
import logging
import requests
from urllib.parse import urlparse, unquote, quote
import uuid
import datetime
from google.cloud import storage
from google.auth.exceptions import DefaultCredentialsError

logger = logging.getLogger(__name__)

# --- Reusable utility from your S3 uploader ---
def get_filename_from_url(url):
    """Extract filename from URL or generate a UUID if not determinable."""
    try:
        path = urlparse(url).path
        filename = os.path.basename(unquote(path))
    except Exception as e:
        logger.warning(f"Could not parse filename from URL '{url}': {e}. Generating UUID.")
        filename = "" # Ensure filename is empty to trigger UUID generation
    
    if not filename: # Catches empty string and None
        filename = f"{uuid.uuid4()}"
        logger.info(f"Generated UUID filename: {filename}")
        
    return filename
# --- End Reusable utility ---

def get_gcs_client():
    """
    Create and return a Google Cloud Storage client.
    Relies on GOOGLE_APPLICATION_CREDENTIALS environment variable for authentication.
    """
    try:
        storage_client = storage.Client()
        return storage_client
    except DefaultCredentialsError:
        logger.error(
            "GOOGLE_APPLICATION_CREDENTIALS not found or invalid. "
            "Please set this environment variable to the path of your GCS service account key JSON file."
        )
        raise
    except Exception as e:
        logger.error(f"Failed to create GCS client: {e}")
        raise

def stream_upload_to_gcs(file_url, custom_filename=None, make_public=False):
    """
    Stream a file from a URL directly to Google Cloud Storage.
    
    Args:
        file_url (str): URL of the file to download.
        custom_filename (str, optional): Custom filename for the uploaded file in GCS.
        make_public (bool, optional): Whether to make the file publicly accessible in GCS.
    
    Returns:
        dict: Information about the uploaded file, similar to the S3 uploader's response.
              {'file_url': str, 'filename': str, 'bucket': str, 'public': bool}
    """
    gcs_client = None
    gcs_bucket_name = os.environ.get('GCS_BUCKET_NAME')

    if not gcs_bucket_name:
        logger.error("GCS_BUCKET_NAME environment variable is not set.")
        raise ValueError("GCS_BUCKET_NAME is required.")

    try:
        gcs_client = get_gcs_client()
        bucket = gcs_client.bucket(gcs_bucket_name)

        if custom_filename:
            destination_blob_name = custom_filename
        else:
            destination_blob_name = get_filename_from_url(file_url)
        
        blob = bucket.blob(destination_blob_name)

        logger.info(f"Attempting to stream from {file_url} to GCS: gs://{gcs_bucket_name}/{destination_blob_name}")

        # Stream the file from URL
        headers = {'User-Agent': 'NCAToolkit-GCS-Uploader/1.0'}
        with requests.get(file_url, stream=True, timeout=60, headers=headers) as response:
            response.raise_for_status() # Raises an HTTPError for bad responses (4XX or 5XX)
            
            # Attempt to get content type from source, default if not available
            content_type = response.headers.get('content-type', 'application/octet-stream')
            logger.info(f"Streaming with Content-Type: {content_type}")

            # The google-cloud-storage library handles chunking and resumable uploads internally
            # when using upload_from_file with a stream.
            # response.raw is a file-like object.
            response.raw.decode_content = True # Ensure that gzip/deflate are handled
            blob.upload_from_file(response.raw, content_type=content_type)

        logger.info(f"Successfully uploaded {destination_blob_name} to bucket {gcs_bucket_name}.")

        final_file_url = ""
        if make_public:
            try:
                blob.make_public()
                logger.info(f"Object gs://{gcs_bucket_name}/{destination_blob_name} made public.")
                # Public URL for GCS objects
                final_file_url = f"https://storage.googleapis.com/{gcs_bucket_name}/{quote(destination_blob_name)}"
            except Exception as e_public:
                logger.error(f"Failed to make gs://{gcs_bucket_name}/{destination_blob_name} public: {e_public}. "
                             "File is uploaded but not public. Generating signed URL instead.")
                # Fallback to signed URL if making public fails
                final_file_url = blob.generate_signed_url(
                    version="v4",
                    expiration=datetime.timedelta(hours=1),
                    method="GET"
                )
                make_public = False # Update status as it's not truly public
        else:
            logger.info(f"Object gs://{gcs_bucket_name}/{destination_blob_name} is private. Generating signed URL.")
            final_file_url = blob.generate_signed_url(
                version="v4",
                expiration=datetime.timedelta(hours=1), # 1 hour expiration
                method="GET"
            )
            
        return {
            'file_url': final_file_url,
            'filename': destination_blob_name,
            'bucket': gcs_bucket_name,
            'public': make_public
        }

    except requests.exceptions.RequestException as e_req:
        logger.error(f"HTTP error downloading file from URL {file_url}: {e_req}")
        raise
    except DefaultCredentialsError: # Catch this specifically if get_gcs_client raises it
        # Already logged in get_gcs_client, re-raise to ensure it stops execution
        raise
    except Exception as e:
        logger.error(f"Error streaming file to GCS (gs://{gcs_bucket_name}/{locals().get('destination_blob_name', 'unknown_blob')}): {e}")
        raise

if __name__ == '__main__':
    # Example Usage (for testing this module directly)
    # You would need to set GOOGLE_APPLICATION_CREDENTIALS and GCS_BUCKET_NAME
    # environment variables before running this.
    logging.basicConfig(level=logging.INFO)
    logger.info("Testing GCS Uploader Module...")

    # Create a dummy file URL for testing (replace with a real one if needed)
    # For local testing, you could run 'python -m http.server 8000' in a directory with a test file.
    # TEST_FILE_URL = "http://localhost:8000/some_test_file.txt" 
    TEST_FILE_URL = "https://www.google.com/images/branding/googlelogo/1x/googlelogo_color_272x92dp.png" # Example public file

    # Ensure environment variables are set for testing
    if not os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        print("Please set the GOOGLE_APPLICATION_CREDENTIALS environment variable.")
    elif not os.getenv("GCS_BUCKET_NAME"):
        print("Please set the GCS_BUCKET_NAME environment variable.")
    else:
        try:
            print(f"Attempting to upload '{TEST_FILE_URL}' (publicly)...")
            result_public = stream_upload_to_gcs(TEST_FILE_URL, custom_filename="test_image_public.png", make_public=True)
            print("Public Upload Result:", result_public)
            
            print(f"\nAttempting to upload '{TEST_FILE_URL}' (privately)...")
            result_private = stream_upload_to_gcs(TEST_FILE_URL, custom_filename="test_image_private.png", make_public=False)
            print("Private Upload Result:", result_private)

        except Exception as e:
            print(f"An error occurred during testing: {e}")

