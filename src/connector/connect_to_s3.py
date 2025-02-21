import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import os

class S3Client:
    def __init__(self, bucket_name):
        self.__bucket_name = bucket_name
        self.__s3 = self.create_client()

    def create_client(self):
        try:
            s3 = boto3.client('s3', region_name='us-east-1')
            print("Connection to S3 successful")
            return s3
        except NoCredentialsError:
            print("Credentials not available")
            return None
        except ClientError as e:
            print(f"Client error: {e}")
            return None


    def list_objects_in_bucket(self):
        try:
            response = self.__s3.list_objects_v2(Bucket=self.__bucket_name)
            if response.get('Contents') is None:
                print("No objects in the bucket")
                return []
            else:
                return [obj.get('Key') for obj in response.get('Contents', [])]
        except ClientError as e:
            print(f"Error: {e}")
            return []

    def download_object(self, object_key, local_path):
        try:
            self.__s3.download_file(self.__bucket_name, object_key, local_path)
            print(f"Downloaded {object_key} to {local_path}")
        except ClientError as e:
            print(f"Error downloading file {object_key}: {e}")

    def upload_object(self, local_path, object_key):
        if not os.path.isfile(local_path):
            print(f"Error: The file {local_path} does not exist.")
            return

        try:
            self.__s3.upload_file(local_path, self.__bucket_name, object_key)
            print(f"Uploaded {local_path} to {object_key}")
        except FileNotFoundError:
            print(f"Error: The file {local_path} was not found.")
        except NoCredentialsError:
            print("Error: No AWS credentials found.")
        except ClientError as e:
            print(f"Error uploading file {local_path}: {e}")
        except Exception as e:
            print(f"Unexpected error: {e}")
