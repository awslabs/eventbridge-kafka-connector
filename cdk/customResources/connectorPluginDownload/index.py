#  /*
#   * // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#   * // SPDX-License-Identifier: Apache-2.0
#   */
#
import requests
import boto3
from smart_open import open

client = boto3.client('kafkaconnect')
s3 = boto3.resource('s3')

def on_event(event, context):

    print(event)
    request_type = event['RequestType']
    if request_type == 'Create': return on_create(event)
    if request_type == 'Update': return on_update(event)
    if request_type == 'Delete': return on_delete(event)

    return

def on_create(event):
    props = event["ResourceProperties"]
    print("create new resource with props %s" % props)

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
    pass

def on_delete(event):
    # Objects will be automatically deleted on destroy bucket
    pass