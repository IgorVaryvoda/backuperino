import os
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import datetime
import tarfile
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Load environment variables
load_dotenv()

# R2 credentials and bucket name
r2_access_key = os.getenv("R2_ACCESS_KEY")
r2_secret_key = os.getenv("R2_SECRET_KEY")
r2_endpoint = os.getenv("R2_ENDPOINT")
bucket_name = os.getenv("R2_BUCKET_NAME")

# Folders to backup
folders_to_backup = [
    "/etc/nginx",
    "/home/igor/sirv-reporting",
    "/var/www/supabase",
    "/var/www/dify",
    "/var/www/viddl.me",
    "/var/www/img.viddl.me",
]

# Initialize R2 client
s3 = boto3.client(
    "s3",
    endpoint_url=r2_endpoint,
    aws_access_key_id=r2_access_key,
    aws_secret_access_key=r2_secret_key,
)


def create_tarball(source_dirs, output_filename):
    """Create a tarball of the specified directories."""
    with tarfile.open(output_filename, "w:gz") as tar:
        for source_dir in source_dirs:
            if os.path.exists(source_dir):
                tar.add(source_dir, arcname=os.path.basename(source_dir))
                logging.info(f"Added {source_dir} to tarball")
            else:
                logging.warning(f"Directory not found: {source_dir}")
    logging.info(f"Tarball created: {output_filename}")


def upload_to_r2(file_name, bucket, object_name=None):
    """Upload a file to an R2 bucket."""
    if object_name is None:
        object_name = os.path.basename(file_name)

    try:
        s3.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    logging.info(f"File uploaded successfully to {bucket}/{object_name}")
    return True


def main():
    # Create a timestamp for the backup
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_filename = f"server_backup_{timestamp}.tar.gz"

    # Create the tarball
    create_tarball(folders_to_backup, backup_filename)

    # Upload to R2
    if upload_to_r2(backup_filename, bucket_name):
        logging.info("Backup completed successfully")
    else:
        logging.error("Backup failed")

    # Clean up the local tarball
    os.remove(backup_filename)
    logging.info(f"Local tarball {backup_filename} removed")


if __name__ == "__main__":
    main()
