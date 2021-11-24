import boto3
from botocore.retries import bucket

""" 
This will perform a transfer of objects between two different aws account, 
it will get the data stream and reupload to the destination bucket.
We could use the copy() method, but it needs our IAM role to have read then 
write access to the source and destination buckets respectively.
"""

# log file
logFile = open('./migration.logs', 'a')

config = {
    'SOURCE_BUCKET_NAME': '',
    'SOURCE_ACCESS_KEY': '',
    'SOURCE_SECRET_KEY': '',
    'DEST_BUCKET_NAME': '',
    'DEST_ACCESS_KEY': '',
    'DEST_SECRET_KEY': ''
}

source_client = boto3.client(
    's3',
    aws_access_key_id=config['SOURCE_ACCESS_KEY'],
    aws_secret_access_key=config['SOURCE_SECRET_KEY'],
)

dest_client = boto3.client(
    's3',
    aws_access_key_id=config['DEST_ACCESS_KEY'],
    aws_secret_access_key=config['DEST_SECRET_KEY'],
)

source_paginator = source_client.get_paginator('list_objects_v2')

# get bucket
# page through prefixed members
response_iterator = source_paginator.paginate(
    Bucket=config['SOURCE_BUCKET_NAME'],
    Prefix='Archive/'
)

for page in response_iterator:
    objects = page['Contents']

    for obj in objects:
        object = source_client.get_object(
            Bucket=config['SOURCE_BUCKET_NAME'],
            Key=obj['Key']
        )

        dest_object = dest_client.get_object(
            Bucket=config['DEST_BUCKET_NAME'],
            Key=obj['Key']
        )

        if dest_object != None:
            if dest_object['StorageClass'] != object['StorageClass']:
                # move object to required storage class
                dest_client.copy(
                    {
                        'Bucket': config['DEST_BUCKET_NAME'],
                        'Key': obj['Key']
                    }, config['DEST_BUCKET_NAME'], obj['Key'],
                    ExtraArgs={
                        'StorageClass': object['StorageClass'],
                        'MetadataDirective': 'COPY'
                    }
                )
        else:
            obj_name = obj['Key']
            obj_size = obj['Size']
            obj_class = obj['StorageClass']
            logFile.write(
                f'Copying to [{obj_class}] of {obj_size}bytes, key={obj_name} ***** ')

            # copy the objects with their meta-data, add x-createdAt timestamp
            dest_client.upload_fileobj(
                object['Body'],
                config['DEST_BUCKET_NAME'],
                obj['Key'],
                {'StorageClass': object['StorageClass'],
                 'Metadata': {**object['Metadata'],
                              'x_last_modified': str(object['last_modified'])}}
            )
            """ dest_client.upload_fileobj(
                object['Body'],
                config['DEST_BUCKET_NAME'],
                obj['Key'],
                {'StorageClass': object['StorageClass'],
                 'Metadata': {**object['Metadata'],
                              'x_last_modified': str(object['last_modified'])}}
            ) """

            logFile.write(f'Done\n')
        # log the copy process

logFile('All Done')
