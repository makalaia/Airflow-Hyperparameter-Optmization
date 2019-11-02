import json
import os

import boto3


class S3:
    BUCKET_NAME = 'test'
    ENDPOINT_URL = 'http://localhost:4572'

    def __init__(self, endpoint_url='http://localhost:4572'):
        self.s3 = boto3.client(
            's3',
            region_name='us-east-1',
            endpoint_url=endpoint_url,
            aws_access_key_id='XXX',
            aws_secret_access_key='XXX',
        )

    def create_bucket(self, bucket=BUCKET_NAME):
        self.s3.create_bucket(Bucket=bucket)

    def delete_bucket(self, bucket=BUCKET_NAME):
        self.s3.delete_bucket(Bucket=bucket)

    def upload_s3_file(self, path, s3_path, bucket=BUCKET_NAME):
        self.s3.upload_file(path, bucket, s3_path)

    def download_s3_file(self, s3_path, path, bucket=BUCKET_NAME):
        self.s3.download_file(bucket, s3_path, path)

    def delete_file(self, s3_path, bucket=BUCKET_NAME):
        self.s3.delete_object(Bucket=bucket, Key=s3_path)

    def delete_files(self, prefix, bucket=BUCKET_NAME):
        files = self.list_s3_files(prefix=prefix, bucket=bucket)
        for i in files:
            self.delete_file(s3_path=i['Key'], bucket=bucket)

    def list_s3_files(self, prefix='', bucket=BUCKET_NAME):
        try:
            files = self.s3.list_objects_v2(Prefix=prefix, Bucket=bucket)['Contents']
        except KeyError:
            return []
        return files

    def put_object(self, data, s3_path, bucket=BUCKET_NAME):
        self.s3.put_object(Body=data, Bucket=bucket, Key=s3_path)

    def get_object(self, s3_path, bucket=BUCKET_NAME):
        obj = self.s3.get_object(Bucket=bucket, Key=s3_path)
        return obj['Body'].read()

    def download_dir(self, prefix, local=None, bucket=BUCKET_NAME):
        if local is None:
            local = os.getcwd()
        paginator = self.s3.get_paginator('list_objects_v2')
        for result in paginator.paginate(Bucket=bucket, Delimiter='/', Prefix=prefix):
            if result.get('CommonPrefixes') is not None:
                for subdir in result.get('CommonPrefixes'):
                    self.download_dir(subdir.get('Prefix'), local, bucket)
            if result.get('Contents') is not None:
                directory = result.get('Contents')[0]['Key']
                # result.get('Contents').pop(0)
                if not os.path.exists(os.path.dirname(local + os.sep + directory)):
                    os.makedirs(os.path.dirname(local + os.sep + directory))
                for file in result.get('Contents'):
                    self.s3.download_file(bucket, file.get('Key'), local + os.sep + file.get('Key'))

