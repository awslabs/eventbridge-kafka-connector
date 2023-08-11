/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

import {Construct} from "constructs";
import * as ec2 from "aws-cdk-lib/aws-ec2";
import * as logs from "aws-cdk-lib/aws-logs";
import {RetentionDays} from "aws-cdk-lib/aws-logs";
import {PolicyStatement,} from "aws-cdk-lib/aws-iam";
import * as path from "path";
import {CustomResource, Duration, RemovalPolicy} from "aws-cdk-lib";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as lambda_python from "@aws-cdk/aws-lambda-python-alpha";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as cr from "aws-cdk-lib/custom-resources";
import * as iam from "aws-cdk-lib/aws-iam";
import * as kafkaconnect from "aws-cdk-lib/aws-kafkaconnect";
// @ts-ignore
import connectorConfig from '../connectorConfigurationJson.json'
export interface ConnectorProps {
    vpc: ec2.Vpc;
    bootstrapServers: string;
    region: string;
    account: string;
    clusterName: string;
    stackName: string;
    connectorRole: iam.Role;
    connectorLogGroup: logs.LogGroup;
    connectorSG: ec2.SecurityGroup;
}
export class Connector extends Construct {

    public readonly securityGroup: ec2.SecurityGroup;

    constructor(scope: Construct, id: string, props: ConnectorProps) {
        super(scope, id);

        const pluginBucket = new s3.Bucket(this, 'pluginBucket', {
            removalPolicy: RemovalPolicy.DESTROY,
            autoDeleteObjects: true,
            blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL
        })

        const pluginDownloadEventHandler = new lambda_python.PythonFunction(this, 'pluginDownloadEventHandler', {
            runtime: lambda.Runtime.PYTHON_3_10,
            entry: path.join(__dirname, '../customResources/connectorPluginDownload'),
            handler: 'on_event',
            timeout: Duration.seconds(60),
            memorySize: 512
        })

        pluginBucket.grantReadWrite(pluginDownloadEventHandler)

        const pluginDownloadProvider = new cr.Provider(this, 'pluginDownloadProvider', {
            onEventHandler: pluginDownloadEventHandler,
            logRetention: logs.RetentionDays.ONE_WEEK
        })

        const pluginDownload = new CustomResource(this, 'pluginDownload', {
            resourceType: 'Custom::MSKConnectPluginDownload',
            serviceToken: pluginDownloadProvider.serviceToken,
            properties: {
                url: 'https://github.com/awslabs/eventbridge-kafka-connector/releases/download/v1.0.0/',
                bucketName: pluginBucket.bucketName,
                key: 'kafka-eventbridge-sink-with-gsr-dependencies.jar',
            }
        })

        const pluginCreationEventHandler = new lambda_python.PythonFunction(this, 'pluginCreationEventHandler', {
            runtime: lambda.Runtime.PYTHON_3_10,
            entry: path.join(__dirname, '../customResources/connectorPlugin'),
            handler: 'on_event',
            timeout: Duration.minutes(14)
        })
        pluginCreationEventHandler.addToRolePolicy(new iam.PolicyStatement({
            actions: [
                'kafkaconnect:CreateCustomPlugin',
                'kafkaconnect:DeleteCustomPlugin',
                'kafkaconnect:UpdateCustomPlugin'
            ],
            resources: [`arn:aws:kafkaconnect:${props.region}:${props.account}:*`]
        }))

        pluginBucket.grantRead(pluginCreationEventHandler)

        const pluginCreationProvider = new cr.Provider(this, 'pluginCreationProvider', {
            onEventHandler: pluginCreationEventHandler,
            logRetention: logs.RetentionDays.ONE_WEEK
        })

        const plugin = new CustomResource(this, 'eventBridgeSinkPlugin', {
            resourceType: 'Custom::MSKConnectPlugin',
            serviceToken: pluginCreationProvider.serviceToken,
            properties: {
                bucketArn: pluginBucket.bucketArn,
                fileKey: pluginDownload.getAtt('key'),
                contentType: 'JAR',
                name: `eventBridgeKafkaConnectorPlugin-${props.stackName}`,
                description: 'Plugin for EventBridgeSink'
            }
        })

        // Connector

        //Set AWS Account ID and AWS region in connector configuration
        const connectorConfiguration = JSON.parse(JSON
            .stringify(connectorConfig)
            .replace("00000000000", props.account)
            .replace("us-east-1", props.region)
            .replace('/default', '/eventbridge-sink-eventbus')
            .replace('default-registry', 'streaming')
        )


        const connector = new kafkaconnect.CfnConnector(this, 'connector', {
            capacity: {
                provisionedCapacity: {
                    mcuCount: 1,
                    workerCount: 1
                }
            },
            connectorDescription: 'Connector for AmazonEventBridgeSink',
            connectorName: 'amazonEventBridgeSink',
            kafkaCluster: {
                apacheKafkaCluster: {
                    bootstrapServers: props.bootstrapServers,
                    vpc: {
                        securityGroups: [props.connectorSG.securityGroupId],
                        subnets: props.vpc.selectSubnets({
                            subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS
                        }).subnetIds
                    }
                }
            },
            logDelivery: {
                workerLogDelivery: {
                    cloudWatchLogs: {
                        enabled: true,
                        logGroup: props.connectorLogGroup.logGroupName
                    }
                }
            },
            kafkaConnectVersion: '2.7.1',
            kafkaClusterClientAuthentication: {
                authenticationType: 'IAM'
            },
            kafkaClusterEncryptionInTransit: {
                encryptionType: 'TLS'
            },
            connectorConfiguration: connectorConfiguration,
            serviceExecutionRoleArn: props.connectorRole.roleArn,
            plugins: [{
                customPlugin: {
                    customPluginArn: plugin.getAttString('arn'),
                    revision: 1
                }
            }]

        })




    }
}