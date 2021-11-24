import math
from typing import Match
import boto3
from botocore.retries import bucket

"""
This will perform a transfer of objects between two different aws account,
it will get the data stream and reupload to the destination bucket.
We could use the copy() method, but it needs our IAM role to have read then
write access to the source and destination buckets respectively.
"""

config = {
    'SOURCE_BUCKET_NAME': '',
    'SOURCE_ACCESS_KEY': '',
    'SOURCE_SECRET_KEY': '',
    'DEST_BUCKET_NAME': '',
    'DEST_ACCESS_KEY': '',
    'DEST_SECRET_KEY': ''
}

countStr = ''
_logFile = None


def get_logger(count, print_count = False):
    """ Logging, count is zero indexed """

    global _logFile
    global countStr
    break_on = 50000 # break logs after every x 
    _count = count // break_on
    _countStr = str(_count) if _count != 0 else ''

    # print count 
    if print_count:
        """ print count on same line if within break_on """
        sep = '\n' if (count % break_on) == 0 else ','
        print(f'{count}{sep}', end=' ')

    if _logFile == None:
        _logFile = open(f'./logs/migration{countStr}.logs', 'a')

    elif _countStr != countStr:
        countStr = _countStr
        _logFile.close()
        _logFile = open(f'./logs/migration{countStr}.logs', 'a')

    return _logFile


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

count = 0
for page in response_iterator:
    objects = page['Contents']

    for obj in objects:
        count += 1
        object = source_client.head_object(
            Bucket=config['SOURCE_BUCKET_NAME'],
            Key=obj['Key']
        )

        obj_name = obj['Key']
        get_logger(count, True).write(f'key={obj_name} ')

        obj_size = obj['Size']
        obj_class = obj['StorageClass']
        get_logger(count).write(
            f'Copying to [{obj_class}] of {obj_size}bytes ***** ')

        conf_obj = {'ACL': 'bucket-owner-full-control',
                    'Metadata': {**object['Metadata'],
                                 'x_last_modified': str(object['LastModified'])}
                    }

        if obj_class:
            conf_obj['StorageClass'] = obj_class

        # copy the objects with their meta-data, add x-createdAt timestamp
        source_client.copy(
            {
                'Bucket': config['SOURCE_BUCKET_NAME'],
                'Key': obj['Key']
            },
            config['DEST_BUCKET_NAME'],
            obj['Key'],
            conf_obj,
        )
        # log the copy process
        get_logger(count).write(f'Done\n')


get_logger(count).write('All Done')
