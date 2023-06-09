/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

import * as cdk from 'aws-cdk-lib';
import {Construct} from 'constructs';
import {RetentionDays} from 'aws-cdk-lib/aws-logs';
import * as cr from 'aws-cdk-lib/custom-resources';
import {AwsCustomResource, AwsCustomResourcePolicy} from 'aws-cdk-lib/custom-resources';
import * as ec2 from "aws-cdk-lib/aws-ec2";
import * as msk from "aws-cdk-lib/aws-msk";

import {Producer} from "./producerConstruct";
import {Peer, Port} from "aws-cdk-lib/aws-ec2";
import {Connector} from "./connectorConstruct";
import {RemovalPolicy} from "aws-cdk-lib";
import * as iam from "aws-cdk-lib/aws-iam";
import * as logs from "aws-cdk-lib/aws-logs";
import {PolicyStatement} from "aws-cdk-lib/aws-iam";
import {Analyzer} from "./analyzerConstruct";
import * as glue from 'aws-cdk-lib/aws-glue';
import * as eb from 'aws-cdk-lib/aws-events';
import {NagSuppressions} from "cdk-nag";


export interface KafkaEventBridgeSinkStackProps extends cdk.StackProps {
    deploymentMode: string;
}

export class KafkaEventBridgeSinkStack extends cdk.Stack {
    constructor(scope: Construct, id: string, props: KafkaEventBridgeSinkStackProps) {
        super(scope, id, props);

        const vpc = new ec2.Vpc(this, 'vpc')

        const mskSG = new ec2.SecurityGroup(this, 'mskSG', {
            securityGroupName: `mskClusterSecurityGroup-${this.stackName}`,
            description: 'Security group for Amazon MSK cluster',
            vpc: vpc
        })

        const cluster = new msk.CfnServerlessCluster(this, 'mskServerless', {
            clientAuthentication: {
                sasl: {
                    iam: {
                        enabled: true,
                    },
                },
            },
            clusterName: `mskServerless-${this.stackName}`,
            vpcConfigs: [{
                subnetIds: vpc.selectSubnets({subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS}).subnetIds,
                securityGroups: [mskSG.securityGroupId],
            }],
        });

        const glueVpcEndpoint = new ec2.InterfaceVpcEndpoint(this, 'glueVpcEndpoint', {
            vpc,
            open: true,
            service: ec2.InterfaceVpcEndpointAwsService.GLUE,
            privateDnsEnabled: true,
        })

        const bootstrapServers = new AwsCustomResource(this, 'bootstrapServers', {
            policy: AwsCustomResourcePolicy.fromSdkCalls({
                resources: cr.AwsCustomResourcePolicy.ANY_RESOURCE
            }),
            installLatestAwsSdk: true,
            logRetention: RetentionDays.ONE_WEEK,
            onCreate: {
                service: 'Kafka',
                action: 'getBootstrapBrokers',
                parameters: {
                    ClusterArn: cluster.attrArn,
                },
                physicalResourceId: cr.PhysicalResourceId.of(`bootstrapServers-${cluster.attrArn}`),
            },
            onUpdate: {
                service: 'kafka',
                action: 'getBootstrapBrokers',
                parameters: {
                    ClusterArn: cluster.attrArn,
                },
            }
        })

        const schemaRegistry = new glue.CfnRegistry(this, 'registry', {
            name: 'streaming',
            description: 'Schema Registry for deploying the EventBridge Sink connector',
        });

        const eventBus = new eb.EventBus(this, 'eventbus', {
           eventBusName: 'eventbridge-sink-eventbus'
        })


        const producer = new Producer(this, 'kafkaProducer', {
            vpc,
            bootstrapServers: bootstrapServers.getResponseField('BootstrapBrokerStringSaslIam'),
            region: this.region,
            account: this.account,
            clusterName: cluster.clusterName,
            schemaRegistry: schemaRegistry,
        })

        const transactionAnalyzer = new Analyzer(this, 'transactionAnalyzer', {
            vpc,
            ecsCluster: producer.ecsCluster,
            bootstrapServers: bootstrapServers.getResponseField('BootstrapBrokerStringSaslIam'),
            region: this.region,
            account: this.account,
            clusterName: cluster.clusterName,
            schemaRegistry: schemaRegistry,
        })

        transactionAnalyzer.node.addDependency(producer)

        mskSG.addIngressRule(producer.securityGroup, Port.tcp(9098))
        mskSG.addIngressRule(transactionAnalyzer.securityGroup, Port.tcp(9098))
        mskSG.addIngressRule(mskSG, ec2.Port.tcp(9098))
        mskSG.addIngressRule(Peer.ipv4(vpc.vpcCidrBlock), Port.tcp(9098))

        const connectorLogGroup = new logs.LogGroup(this, 'connectorLogGroup', {
            logGroupName: '/aws/mskconnect/eventBridgeSinkConnector',
            removalPolicy: RemovalPolicy.DESTROY,
            retention: RetentionDays.ONE_WEEK
        })

        const connectorRole = new iam.Role(this, 'connectorRole', {
            assumedBy: new iam.ServicePrincipal('kafkaconnect.amazonaws.com')
        })

        const connectorSG = new ec2.SecurityGroup(this, 'connectorSG', {
            securityGroupName: `mskConnectSecurityGroup-${props.stackName}`,
            description: 'Security group for Amazon MSK Connect',
            vpc
        })



        if (props.deploymentMode === 'FULL') {
            const connector = new Connector(this, 'connector', {
                vpc,
                bootstrapServers: bootstrapServers.getResponseField('BootstrapBrokerStringSaslIam'),
                region: this.region,
                account: this.account,
                clusterName: cluster.clusterName,
                stackName: this.stackName,
                connectorSG,
                connectorRole,
                connectorLogGroup
            })
        }

        connectorRole.addToPolicy(new iam.PolicyStatement({
            actions: ['events:PutEvents'],
            resources: [`arn:aws:events:${this.region}:${this.account}:event-bus/eventbridge-sink-eventbus`]
        }))

        connectorRole.addToPolicy(new iam.PolicyStatement({
            actions: ['glue:GetSchemaVersion'],
            resources: [`*`]
        }))

        connectorRole.addToPolicy(new PolicyStatement({
            actions: [
                "kafka-cluster:Connect",
                "kafka-cluster:AlterCluster",
                "kafka-cluster:DescribeCluster"
            ],
            resources: [`arn:aws:kafka:${this.region}:${this.account}:cluster/${cluster.clusterName}/*`]
        }))

        connectorRole.addToPolicy(new PolicyStatement({
            actions: [
                "kafka-cluster:*Topic*",
                "kafka-cluster:WriteData",
                "kafka-cluster:ReadData"
            ],
            resources: [`arn:aws:kafka:${this.region}:${this.account}:topic/${cluster.clusterName}/*`]
        }))

        connectorRole.addToPolicy(new PolicyStatement({
            actions: [
                "kafka-cluster:AlterGroup",
                "kafka-cluster:DescribeGroup"
            ],
            resources: [`arn:aws:kafka:${this.region}:${this.account}:group/${cluster.clusterName}/*`]
        }))












    }
}
