import boto3 as boto
from botocore.exceptions import ClientError
import s3fs

def list_datasets():
    s3 = boto3.resource('s3')
    data_buckets = []
    for bucket in s3.buckets.all():
        try:
            tags = {el["Key"]: el["Value"] for el in bucket.Tagging().tag_set} # wtf boto?
            if tags["collection"] == "modata":
                data_buckets.append(bucket.name)
        except ClientError:
            pass
        
    return data_buckets


def get_files(dataset, filename):
    s3 = s3fs.S3FileSystem(anon=True)
    return s3.glob(dataset + '/*/' + filename)