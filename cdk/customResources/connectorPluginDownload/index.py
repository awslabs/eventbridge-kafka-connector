#
#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#  SPDX-License-Identifier: Apache-2.0
#
#

import requests
import boto3
import logging
from smart_open import open

logger = logging.getLogger()
logger.setLevel(logging.INFO)

client = boto3.client('kafkaconnect')
s3 = boto3.resource('s3')

def on_event(event, context):

    logger.info(event)
    request_type = event['RequestType']
    match request_type:
        case 'Create':
            return on_create(event)
        case 'Update':
            return on_update(event)
        case 'Delete':
            return on_delete(event)
        case _:
            logger.error(f'Unexpected RequestType: {event["RequestType"]}')

    return

def on_create(event):
    props = event["ResourceProperties"]
    logger.info("create new resource with props %s" % props)

    URL = props['url'] + props['key']

    response = requests.get(URL, stream=True)
    s3url = f's3://{props["bucketName"]}/{props["key"]}'
    with open(s3url, 'wb') as fout:
        fout.write(response.content)

    physical_id = f'plugin--{props["bucketName"]}--{props["key"]}'
    return {
        'PhysicalResourceId': physical_id,
        'Data': {
            'bucket': props["bucketName"],
            'key': props["key"]
        }
    }

def on_update(event):
    # Updating the plugin jar is not supported in this custom resource
    pass

def on_delete(event):
    # Objects will be automatically deleted on destroy bucket
    pass