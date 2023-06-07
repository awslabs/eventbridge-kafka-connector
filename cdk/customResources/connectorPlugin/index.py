#
#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#  SPDX-License-Identifier: Apache-2.0
#
#

import boto3
import time

client = boto3.client('kafkaconnect')


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

    response = client.create_custom_plugin(
        contentType=props['contentType'],
        location={
            's3Location': {
                'bucketArn': props['bucketArn'],
                'fileKey': props['fileKey'],
            }
        },
        name=props['name'],
        description=props['description']
    )

    physical_id = f'{response["customPluginArn"]}'

    return {
        'PhysicalResourceId': physical_id,
        'Data': {
            'arn': response['customPluginArn'],
            'revision': response['revision']
        }
    }


def on_update(event):
    pass


def on_delete(event):
    physical_id = event["PhysicalResourceId"]
    print("delete resource with physical id %s" % physical_id)
    connector_active = True
    while connector_active:
        try:
            response = client.delete_custom_plugin(
                customPluginArn=physical_id
            )
            connector_active = False
        except:
            time.sleep(10000)
