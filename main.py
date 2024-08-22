import os
import boto3
from botocore.exceptions import ClientError
import datetime
import tarfile
import logging
import sys

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# R2 credentials and bucket name
r2_access_key = os.environ.get("R2_ACCESS_KEY")
r2_secret_key = os.environ.get("R2_SECRET_KEY")
r2_endpoint = os.environ.get("R2_ENDPOINT")
bucket_name = os.environ.get("R2_BUCKET_NAME")

# Check for required environment variables
required_env_vars = ["R2_ACCESS_KEY", "R2_SECRET_KEY", "R2_ENDPOINT", "R2_BUCKET_NAME"]
missing_vars = [var for var in required_env_vars if not os.environ.get(var)]
if missing_vars:
    logging.error(f"Missing required environment variables: {', '.join(missing_vars)}")
    sys.exit(1)

# Folders to backup
folders_to_backup = []
try:
    with open('folders.txt', 'r') as file:
        folders_to_backup = [line.strip() for line in file if line.strip()]
    if not folders_to_backup:
        logging.error("No folders specified in folders.txt")
        sys.exit(1)
except FileNotFoundError:
    logging.error("folders.txt not found")
    sys.exit(1)

# Initialize R2 client
try:
    s3 = boto3.client(
        service_name='s3',
        endpoint_url=r2_endpoint,
        aws_access_key_id=r2_access_key,
        aws_secret_access_key=r2_secret_key,
    )
except Exception as e:
    logging.error(f"Failed to initialize R2 client: {e}")
    sys.exit(1)

def create_tarball(source_dirs, output_filename):
    """Create a tarball of the specified directories."""
    try:
        with tarfile.open(output_filename, "w:gz") as tar:
            for source_dir in source_dirs:
                if os.path.exists(source_dir):
                    tar.add(source_dir, arcname=os.path.basename(source_dir))
                    logging.info(f"Added {source_dir} to tarball")
                else:
                    logging.warning(f"Directory not found: {source_dir}")
        logging.info(f"Tarball created: {output_filename}")
    except Exception as e:
        logging.error(f"Error creating tarball: {e}")
        raise

def upload_to_r2(file_name, bucket, object_name=None):
    """Upload a file to an R2 bucket."""
    if object_name is None:
        object_name = os.path.basename(file_name)

    try:
        s3.upload_file(file_name, str(bucket), object_name)
        logging.info(f"File uploaded successfully to {bucket}/{object_name}")
        return True
    except ClientError as e:
        logging.error(f"ClientError in upload_to_r2: {e}")
    except Exception as e:
        logging.error(f"Unexpected error in upload_to_r2: {e}")
    return False

def split_file(file_path, chunk_size=3.9 * 1024 * 1024 * 1024):  # 3.9 GB chunks
    """Split a file into smaller chunks."""
    base_name = os.path.basename(file_path)
    chunk_names = []
    try:
        with open(file_path, 'rb') as f:
            chunk = 0
            while True:
                chunk_data = f.read(int(chunk_size))
                if not chunk_data:
                    break
                chunk_name = f"{base_name}.part{chunk:03d}"
                with open(chunk_name, 'wb') as chunk_file:
                    chunk_file.write(chunk_data)
                logging.info(f"Created chunk: {chunk_name}")
                chunk_names.append(chunk_name)
                chunk += 1
        return chunk_names
    except Exception as e:
        logging.error(f"Error splitting file: {e}")
        raise

def main():
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_filename = f"server_backup_{timestamp}.tar.gz"

    try:
        create_tarball(folders_to_backup, backup_filename)
        chunk_names = split_file(backup_filename)

        all_uploads_successful = True
        for chunk_name in chunk_names:
            if upload_to_r2(chunk_name, bucket_name):
                logging.info(f"Chunk {chunk_name} uploaded successfully")
                os.remove(chunk_name)
                logging.info(f"Local chunk {chunk_name} removed")
            else:
                logging.error(f"Failed to upload chunk {chunk_name}")
                all_uploads_successful = False

        if all_uploads_successful:
            logging.info("Backup completed successfully")
        else:
            logging.error("Backup failed")

    except Exception as e:
        logging.error(f"An error occurred during the backup process: {e}")
    finally:
        if os.path.exists(backup_filename):
            os.remove(backup_filename)
            logging.info(f"Local tarball {backup_filename} removed")

if __name__ == "__main__":
    main()
