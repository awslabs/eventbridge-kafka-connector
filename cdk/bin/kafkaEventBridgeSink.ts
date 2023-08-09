#!/usr/bin/env node

/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

import * as cdk from 'aws-cdk-lib';
import { KafkaEventBridgeSinkStack } from '../lib/kafkaEventBridgeSinkStack';
import {Aspects} from "aws-cdk-lib";
import {AwsSolutionsChecks} from "cdk-nag";


const app = new cdk.App();
const deploymentMode = app.node.tryGetContext('deploymentMode')
Aspects.of(app).add(new AwsSolutionsChecks({ verbose: true }))
new KafkaEventBridgeSinkStack(app, 'KafkaEventBridgeSinkStack', {
    deploymentMode: deploymentMode
});