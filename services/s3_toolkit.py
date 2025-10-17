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
import mimetypes
import boto3
import logging
from urllib.parse import quote

DEFAULT_CONTENT_TYPE = 'application/octet-stream'
GENERIC_CONTENT_TYPES = {DEFAULT_CONTENT_TYPE, 'binary/octet-stream'}

# Register additional common MIME types that mimetypes may not know about by default.
mimetypes.add_type('application/x-subrip', '.srt')


logger = logging.getLogger(__name__)

def _clean_content_type_value(value):
    if not value:
        return None
    cleaned = value.strip()
    return cleaned or None


def _is_generic_content_type(value):
    if not value:
        return True
    base_type = value.split(';', 1)[0].strip().lower()
    return base_type in GENERIC_CONTENT_TYPES


def resolve_content_type(filename, provided_content_type=None):
    """Resolve the most appropriate Content-Type for an upload."""
    provided = _clean_content_type_value(provided_content_type)
    if provided and not _is_generic_content_type(provided):
        return provided

    guessed, _ = mimetypes.guess_type(filename)
    if guessed:
        return guessed

    return DEFAULT_CONTENT_TYPE


def upload_to_s3(file_path, s3_url, access_key, secret_key, bucket_name, region):
    # Parse the S3 URL into bucket, region, and endpoint
    #bucket_name, region, endpoint_url = parse_s3_url(s3_url)
    
    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region
    )
    
    client = session.client('s3', endpoint_url=s3_url)

    try:
        # Upload the file to the specified S3 bucket
        with open(file_path, 'rb') as data:
            filename = os.path.basename(file_path)
            content_type = resolve_content_type(filename)
            client.upload_fileobj(
                data,
                bucket_name,
                filename,
                ExtraArgs={'ACL': 'public-read', 'ContentType': content_type}
            )

        # URL encode the filename for the URL
        encoded_filename = quote(os.path.basename(file_path))
        file_url = f"{s3_url}/{bucket_name}/{encoded_filename}"
        return file_url
    except Exception as e:
        logger.error(f"Error uploading file to S3: {e}")
        raise
