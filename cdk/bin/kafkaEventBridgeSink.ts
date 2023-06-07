#!/usr/bin/env node

/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

import * as cdk from 'aws-cdk-lib';
import { KafkaEventBridgeSinkStack } from '../lib/kafkaEventBridgeSinkStack';


const app = new cdk.App();
const deploymentMode = app.node.tryGetContext('deploymentMode')
new KafkaEventBridgeSinkStack(app, 'KafkaEventBridgeSinkStack', {
    deploymentMode: deploymentMode
});