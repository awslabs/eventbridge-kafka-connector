#
#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#  SPDX-License-Identifier: Apache-2.0
#
#

import boto3
import time
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

client = boto3.client('kafkaconnect')


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
    # Updating a connector plugin is not supported in this custom resource
    pass


def on_delete(event):
    physical_id = event["PhysicalResourceId"]
    logger.info("delete resource with physical id %s" % physical_id)
    connector_active = True
    retry_count = 0
    while connector_active & retry_count > 50:
        try:
            response = client.delete_custom_plugin(
                customPluginArn=physical_id
            )
            connector_active = False
        except:
            time.sleep(10000) #Wait for 10 seconds to check again if connector is still active
            retry_count += 1
