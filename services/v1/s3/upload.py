# Copyright (c) 2025 Stephen G. Pope
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.


import os
import boto3
import logging
import requests
from urllib.parse import urlparse, unquote, quote
import uuid
# import re # This import was not used, can be removed if not needed elsewhere

logger = logging.getLogger(__name__)

def get_s3_client():
    """Create and return an S3 client using environment variables."""
    endpoint_url = os.getenv('S3_ENDPOINT_URL')
    access_key = os.getenv('S3_ACCESS_KEY')
    secret_key = os.getenv('S3_SECRET_KEY')
    # Ensure region_name is correctly fetched. If S3_REGION might be empty,
    # boto3 might default, but explicit is often better.
    # For GCS, 'us-east-1' is a common region to use with the global endpoint
    # if your client library requires a region for signing.
    region = os.getenv('S3_REGION', 'us-east-1') # Default to us-east-1 if not set

    # It's good practice to check if essential configs are present
    if not endpoint_url:
        logger.error("S3_ENDPOINT_URL is not set.")
        raise ValueError("S3_ENDPOINT_URL is required.")
    if not access_key:
        logger.error("S3_ACCESS_KEY is not set.")
        raise ValueError("S3_ACCESS_KEY is required.")
    if not secret_key:
        logger.error("S3_SECRET_KEY is not set.")
        raise ValueError("S3_SECRET_KEY is required.")

    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region # Boto3 uses this for request signing (SigV4)
    )
    
    # For GCS, you might need to explicitly set the signature version
    # if the default isn't working as expected, though usually boto3 handles this.
    # from botocore.client import Config
    # s3_client = session.client('s3', endpoint_url=endpoint_url, config=Config(signature_version='s3v4'))
    s3_client = session.client('s3', endpoint_url=endpoint_url)
    return s3_client

def get_filename_from_url(url):
    """Extract filename from URL."""
    try:
        path = urlparse(url).path
        filename = os.path.basename(unquote(path))
    except Exception as e:
        logger.warning(f"Could not parse filename from URL '{url}': {e}. Generating UUID.")
        filename = "" # Ensure filename is empty to trigger UUID generation
    
    # If filename cannot be determined or is empty after parsing, generate a UUID
    if not filename: # Catches empty string and None
        filename = f"{uuid.uuid4()}"
        logger.info(f"Generated UUID filename: {filename}")
        
    return filename

def stream_upload_to_s3(file_url, custom_filename=None, make_public=False):
    """
    Stream a file from a URL directly to S3 without saving to disk.
    
    Args:
        file_url (str): URL of the file to download
        custom_filename (str, optional): Custom filename for the uploaded file
        make_public (bool, optional): Whether to make the file publicly accessible
    
    Returns:
        dict: Information about the uploaded file
    """
    try:
        # Get S3 configuration
        bucket_name = os.environ.get('S3_BUCKET_NAME')
        if not bucket_name:
            logger.error("S3_BUCKET_NAME is not set.")
            raise ValueError("S3_BUCKET_NAME is required.")
            
        endpoint_url = os.getenv('S3_ENDPOINT_URL') # Needed for constructing public URL later
        
        # Get S3 client
        s3_client = get_s3_client()
        
        # Determine filename (use custom if provided, otherwise extract from URL)
        if custom_filename:
            filename = custom_filename
        else:
            filename = get_filename_from_url(file_url)
        
        # Start a multipart upload
        logger.info(f"Starting multipart upload for {filename} to bucket {bucket_name}")
        
        # --- MODIFICATION START ---
        # Removed ACL from create_multipart_upload
        # The ACL parameter was likely causing the "InvalidArgument" error with GCS.
        # GCS generally prefers IAM permissions over S3 ACLs, or ACLs set after upload.
        multipart_upload = s3_client.create_multipart_upload(
            Bucket=bucket_name,
            Key=filename
            # ACL=acl # Removed this line
        )
        # --- MODIFICATION END ---
        
        upload_id = multipart_upload['UploadId']
        
        # Stream the file from URL
        # Adding a timeout and User-Agent is good practice for HTTP requests
        headers = {'User-Agent': 'NCAToolkit/1.0'}
        response = requests.get(file_url, stream=True, timeout=30, headers=headers) # 30-second timeout
        response.raise_for_status() # Raises an HTTPError for bad responses (4XX or 5XX)
        
        # Process in chunks using multipart upload
        # GCS (like S3) requires parts to be at least 5MB, except for the last part.
        chunk_size_for_s3_part = 5 * 1024 * 1024  # 5MB
        parts = []
        part_number = 1
        
        buffer = bytearray()
        
        # Read from stream in smaller chunks, accumulate up to S3 part size
        for http_chunk in response.iter_content(chunk_size=1 * 1024 * 1024):  # Read 1MB at a time from HTTP stream
            if http_chunk: # filter out keep-alive new chunks
                buffer.extend(http_chunk)
            
            # When we have enough data for an S3 part, or if the stream is ending and there's data
            while len(buffer) >= chunk_size_for_s3_part:
                current_part_data = buffer[:chunk_size_for_s3_part]
                buffer = buffer[chunk_size_for_s3_part:]

                logger.info(f"Uploading part {part_number} (size: {len(current_part_data)} bytes)")
                part = s3_client.upload_part(
                    Bucket=bucket_name,
                    Key=filename,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=current_part_data
                )
                
                parts.append({
                    'PartNumber': part_number,
                    'ETag': part['ETag']
                })
                
                part_number += 1
        
        # Upload any remaining data as the final part
        if buffer: # If there's anything left in the buffer, it's the last part
            logger.info(f"Uploading final part {part_number} (size: {len(buffer)} bytes)")
            part = s3_client.upload_part(
                Bucket=bucket_name,
                Key=filename,
                PartNumber=part_number,
                UploadId=upload_id,
                Body=buffer # Send the remainder
            )
            
            parts.append({
                'PartNumber': part_number,
                'ETag': part['ETag']
            })
        
        # Complete the multipart upload
        logger.info("Completing multipart upload")
        s3_client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=filename,
            UploadId=upload_id,
            MultipartUpload={'Parts': parts}
        )

        # --- ACL MODIFICATION SUGGESTION ---
        # If you need to make the object public, do it *after* the upload is complete.
        if make_public:
            try:
                logger.info(f"Setting ACL for {filename} to public-read")
                s3_client.put_object_acl(
                    ACL='public-read',
                    Bucket=bucket_name,
                    Key=filename
                )
                # Construct the public URL. Ensure endpoint_url is the base (e.g., https://storage.googleapis.com)
                encoded_filename = quote(filename) # URL encode the filename for the final URL
                final_file_url = f"{endpoint_url.rstrip('/')}/{bucket_name}/{encoded_filename}"
            except Exception as e_acl:
                logger.error(f"Could not set ACL to public-read for {filename}: {e_acl}. File is uploaded but not public.")
                # Fallback to presigned URL or indicate it's not public
                final_file_url = s3_client.generate_presigned_url(
                    'get_object',
                    Params={'Bucket': bucket_name, 'Key': filename},
                    ExpiresIn=3600  # URL expires in 1 hour
                )
                make_public = False # Update status
        else:
            # Generate a pre-signed URL for private files
            final_file_url = s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': bucket_name, 'Key': filename},
                ExpiresIn=3600  # URL expires in 1 hour
            )
        
        return {
            'file_url': final_file_url,
            'filename': filename, 
            'bucket': bucket_name,
            'public': make_public
        }
        
    except requests.exceptions.RequestException as e_req:
        logger.error(f"HTTP error downloading file from URL {file_url}: {e_req}")
        raise # Re-raise the exception to be handled by the caller
    except boto3.exceptions.Boto3Error as e_boto: # More specific Boto3 exception
        logger.error(f"Boto3 error during S3 operation: {e_boto}")
        # Attempt to abort the multipart upload if it was initiated
        if 'upload_id' in locals() and upload_id:
            try:
                logger.info(f"Attempting to abort failed multipart upload {upload_id}")
                s3_client.abort_multipart_upload(
                    Bucket=bucket_name, Key=filename, UploadId=upload_id
                )
                logger.info(f"Successfully aborted multipart upload {upload_id}")
            except Exception as e_abort:
                logger.error(f"Failed to abort multipart upload {upload_id}: {e_abort}")
        raise
    except Exception as e:
        logger.error(f"Generic error streaming file to S3: {e}")
        # Attempt to abort the multipart upload if it was initiated (if applicable for generic errors too)
        if 's3_client' in locals() and 'bucket_name' in locals() and 'filename' in locals() and 'upload_id' in locals() and upload_id:
             try:
                logger.info(f"Attempting to abort failed multipart upload {upload_id} due to generic error.")
                s3_client.abort_multipart_upload(
                    Bucket=bucket_name, Key=filename, UploadId=upload_id
                )
                logger.info(f"Successfully aborted multipart upload {upload_id}")
             except Exception as e_abort:
                logger.error(f"Failed to abort multipart upload {upload_id}: {e_abort}")
        raise
