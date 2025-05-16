# Copyright (c) 2025 Stephen G. Pope & hcthisen
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
import boto3 # Using boto3 for GCS interoperability
import logging
import requests
from urllib.parse import urlparse, unquote, quote
import uuid
from botocore.client import Config # For specifying signature version if needed

logger = logging.getLogger(__name__)

def get_gcs_client():
    """Create and return a GCS client using environment variables (S3 compatibility mode)."""
    # For GCS, the endpoint URL for S3 compatibility is typically https://storage.googleapis.com
    endpoint_url = os.getenv('GCS_ENDPOINT_URL', 'https://storage.googleapis.com')
    access_key = os.getenv('GCS_ACCESS_KEY')
    secret_key = os.getenv('GCS_SECRET_KEY')
    # Region is not strictly necessary for GCS global endpoint but boto3 might expect it.
    # It's often ignored by GCS when using the global endpoint with HMAC keys.
    # However, it can be relevant for request signing. 'auto' or a specific region can be used.
    # For GCS, if a region is required by boto3 for signing and you're using global endpoint,
    # a common placeholder like 'us-east1' might be used, but GCS itself doesn't use it
    # in the same way AWS S3 does for bucket location with global HMAC keys.
    # Let's make it configurable or default to 'auto' if boto3 handles it well.
    region = os.getenv('GCS_REGION', 'auto')


    if not access_key:
        logger.error("GCS_ACCESS_KEY is not set.")
        raise ValueError("GCS_ACCESS_KEY is required for GCS interoperability.")
    if not secret_key:
        logger.error("GCS_SECRET_KEY is not set.")
        raise ValueError("GCS_SECRET_KEY is required for GCS interoperability.")
    if not endpoint_url: # Should default, but good to check if user overrode it to empty
        logger.error("GCS_ENDPOINT_URL is not set (though it has a default).")
        raise ValueError("GCS_ENDPOINT_URL is required.")

    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region if region != 'auto' else None # Pass None if 'auto' to let boto3 decide
    )
    
    # For GCS, specifying s3v4 signature version is often a good idea.
    # Some regions or newer buckets might require it.
    gcs_client = session.client(
        's3', # Still 's3' due to interoperability mode
        endpoint_url=endpoint_url,
        config=Config(signature_version='s3v4')
    )
    return gcs_client

def get_filename_from_url(url):
    """Extract filename from URL."""
    try:
        path = urlparse(url).path
        filename = os.path.basename(unquote(path))
    except Exception as e:
        logger.warning(f"Could not parse filename from URL '{url}': {e}. Generating UUID.")
        filename = "" 
    
    if not filename: 
        filename = f"{uuid.uuid4()}" # Generate a UUID if no filename
        # Attempt to get extension from URL if possible, otherwise no extension
        try:
            parsed_url = urlparse(url)
            original_filename = os.path.basename(unquote(parsed_url.path))
            if '.' in original_filename:
                extension = original_filename.rsplit('.', 1)[1]
                if len(extension) < 10: # Basic sanity check for extension length
                    filename = f"{filename}.{extension}"
        except Exception:
            pass # Ignore if extension can't be parsed
        logger.info(f"Generated UUID filename: {filename}")
        
    return filename

def stream_upload_to_gcs(file_url, custom_filename=None, make_public=False):
    """
    Stream a file from a URL directly to GCS (using S3 compatibility) without saving to disk.
    
    Args:
        file_url (str): URL of the file to download
        custom_filename (str, optional): Custom filename for the uploaded file
        make_public (bool, optional): Whether to make the file publicly accessible
    
    Returns:
        dict: Information about the uploaded file
    """
    try:
        bucket_name = os.environ.get('GCS_BUCKET_NAME')
        if not bucket_name:
            logger.error("GCS_BUCKET_NAME is not set.")
            raise ValueError("GCS_BUCKET_NAME is required.")
            
        # Endpoint URL for constructing public URL. For GCS, this is typically storage.googleapis.com
        # Or for direct path-style access: https://storage.googleapis.com/BUCKET_NAME/OBJECT_NAME
        gcs_public_base_url = os.getenv('GCS_PUBLIC_URL_BASE', 'https://storage.googleapis.com')
        
        gcs_client = get_gcs_client()
        
        if custom_filename:
            filename = custom_filename
        else:
            filename = get_filename_from_url(file_url)
        
        logger.info(f"Starting multipart upload for {filename} to GCS bucket {bucket_name}")
        
        # GCS with S3 interop does not support the 'ACL' parameter directly in create_multipart_upload.
        # ACLs should be set via put_object_acl after the upload.
        multipart_upload = gcs_client.create_multipart_upload(
            Bucket=bucket_name,
            Key=filename
            # ContentType can be set here if known, or later with put_object_acl, or GCS might infer it.
        )
        
        upload_id = multipart_upload['UploadId']
        
        headers = {'User-Agent': 'NCAToolkit-GCS-Upload/1.0'}
        response = requests.get(file_url, stream=True, timeout=60, headers=headers) # Increased timeout
        response.raise_for_status()
        
        # GCS (like S3) requires parts to be at least 5MB, except for the last part.
        chunk_size_for_gcs_part = 5 * 1024 * 1024  # 5MB
        parts = []
        part_number = 1
        
        buffer = bytearray()
        
        for http_chunk in response.iter_content(chunk_size=1 * 1024 * 1024):  # Read 1MB at a time
            if http_chunk:
                buffer.extend(http_chunk)
            
            while len(buffer) >= chunk_size_for_gcs_part:
                current_part_data = buffer[:chunk_size_for_gcs_part]
                buffer = buffer[chunk_size_for_gcs_part:]

                logger.info(f"Uploading GCS part {part_number} (size: {len(current_part_data)} bytes)")
                part = gcs_client.upload_part(
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
        
        if buffer: 
            logger.info(f"Uploading final GCS part {part_number} (size: {len(buffer)} bytes)")
            part = gcs_client.upload_part(
                Bucket=bucket_name,
                Key=filename,
                PartNumber=part_number,
                UploadId=upload_id,
                Body=buffer
            )
            
            parts.append({
                'PartNumber': part_number,
                'ETag': part['ETag']
            })
        
        logger.info("Completing GCS multipart upload")
        gcs_client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=filename,
            UploadId=upload_id,
            MultipartUpload={'Parts': parts}
        )

        final_file_url = ""
        if make_public:
            try:
                logger.info(f"Setting ACL for GCS object {filename} to public-read")
                # For GCS, 'public-read' is a standard canned ACL
                gcs_client.put_object_acl(
                    ACL='public-read',
                    Bucket=bucket_name,
                    Key=filename
                )
                encoded_filename = quote(filename)
                final_file_url = f"{gcs_public_base_url.rstrip('/')}/{bucket_name}/{encoded_filename}"
            except Exception as e_acl:
                logger.error(f"Could not set ACL to public-read for GCS object {filename}: {e_acl}. File is uploaded but not public.")
                # Fallback to signed URL or indicate it's not public
                # GCS signed URLs through boto3 use S3's mechanism
                final_file_url = gcs_client.generate_presigned_url(
                    'get_object',
                    Params={'Bucket': bucket_name, 'Key': filename},
                    ExpiresIn=3600  # URL expires in 1 hour
                )
                make_public = False # Update status
        else:
            # Generate a pre-signed URL for private files
            final_file_url = gcs_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': bucket_name, 'Key': filename},
                ExpiresIn=3600  # URL expires in 1 hour
            )
        
        return {
            'file_url': final_file_url,
            'filename': filename,  
            'bucket': bucket_name,
            'public': make_public,
            'storage_provider': 'GCS'
        }
        
    except requests.exceptions.RequestException as e_req:
        logger.error(f"HTTP error downloading file from URL {file_url} for GCS upload: {e_req}")
        raise 
    except boto3.exceptions.Boto3Error as e_boto: # Covers botocore.exceptions.ClientError as well
        logger.error(f"Boto3/GCS client error during GCS operation: {e_boto}")
        if 'upload_id' in locals() and upload_id and 'gcs_client' in locals() and gcs_client:
            try:
                logger.info(f"Attempting to abort failed GCS multipart upload {upload_id}")
                gcs_client.abort_multipart_upload(
                    Bucket=bucket_name, Key=filename, UploadId=upload_id
                )
                logger.info(f"Successfully aborted GCS multipart upload {upload_id}")
            except Exception as e_abort:
                logger.error(f"Failed to abort GCS multipart upload {upload_id}: {e_abort}")
        raise
    except Exception as e:
        logger.error(f"Generic error streaming file to GCS: {e}")
        if 'gcs_client' in locals() and gcs_client and 'bucket_name' in locals() and bucket_name and 'filename' in locals() and filename and 'upload_id' in locals() and upload_id:
             try:
                logger.info(f"Attempting to abort failed GCS multipart upload {upload_id} due to generic error.")
                gcs_client.abort_multipart_upload(
                    Bucket=bucket_name, Key=filename, UploadId=upload_id
                )
                logger.info(f"Successfully aborted GCS multipart upload {upload_id}")
             except Exception as e_abort:
                logger.error(f"Failed to abort GCS multipart upload {upload_id}: {e_abort}")
        raise

# Example usage (for testing, typically called from an API endpoint)
# if __name__ == '__main__':
#     logging.basicConfig(level=logging.INFO)
#     # Set these environment variables before running
#     # os.environ['GCS_ACCESS_KEY'] = 'YOUR_GCS_HMAC_ACCESS_KEY'
#     # os.environ['GCS_SECRET_KEY'] = 'YOUR_GCS_HMAC_SECRET_KEY'
#     # os.environ['GCS_BUCKET_NAME'] = 'your-gcs-bucket-name'
#     
#     test_url = "https://www.w3.org/WAI/ER/tests/xhtml/testfiles/resources/pdf/dummy.pdf" # A small PDF file for testing
#     custom_test_filename = "my_custom_gcs_test_upload.pdf"
#     
#     try:
#         # Test private upload
#         # result = stream_upload_to_gcs(test_url, custom_filename=custom_test_filename, make_public=False)
#         # logger.info(f"GCS Upload successful (private): {result}")
#
#         # Test public upload
#         result_public = stream_upload_to_gcs(test_url, custom_filename="public_gcs_file.pdf", make_public=True)
#         logger.info(f"GCS Upload successful (public): {result_public}")
#
#     except Exception as e:
#         logger.error(f"GCS upload failed: {e}")
