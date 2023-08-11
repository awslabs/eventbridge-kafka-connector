/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

import {Construct} from "constructs";
import * as ec2 from "aws-cdk-lib/aws-ec2";
import * as ssm from "aws-cdk-lib/aws-ssm";
import * as ecs from "aws-cdk-lib/aws-ecs";
import {Compatibility, ContainerImage, LogDriver} from "aws-cdk-lib/aws-ecs";
import {PolicyStatement, Role, ServicePrincipal} from "aws-cdk-lib/aws-iam";
import * as path from "path";
import {aws_glue, aws_ssm} from "aws-cdk-lib";

export interface AnalyzerProps {
    ecsCluster: ecs.Cluster;
    vpc: ec2.Vpc;
    region: string;
    account: string;
    clusterName: string;
    bootstrapServersParameter: aws_ssm.StringParameter;
    schemaRegistry: aws_glue.CfnRegistry;
    schemaRegistryName: aws_ssm.StringParameter;
    eventsTopic: aws_ssm.StringParameter;
    notificationTopic: aws_ssm.StringParameter;
}

export class Analyzer extends Construct {

    public readonly securityGroup: ec2.SecurityGroup;
    public readonly taskRole: Role;

    constructor(scope: Construct, id: string, props: AnalyzerProps) {
        super(scope, id);

        this.securityGroup = new ec2.SecurityGroup(this, 'securityGroup', {
            vpc: props.vpc
        })

        this.taskRole = new Role(this, 'taskRole', {
            assumedBy: new ServicePrincipal('ecs-tasks.amazonaws.com')
        })

        const taskDefinition = new ecs.TaskDefinition(this, 'taskDefinition', {
            compatibility: Compatibility.FARGATE,
            cpu: '1024',
            memoryMiB: '2048',
            taskRole: this.taskRole
        })


        taskDefinition.addContainer('container', {
            image: ContainerImage.fromAsset(path.join(__dirname, '../transactionAnalyzer')),
            logging: LogDriver.awsLogs({
                streamPrefix: 'kafkaProducer',
            }),
            secrets: {
                'NOTIFICATIONS_TOPIC_NAME': ecs.Secret.fromSsmParameter(props.notificationTopic),
                'TOPIC_NAME': ecs.Secret.fromSsmParameter(props.eventsTopic),
                'SCHEMA_REGISTRY_NAME': ecs.Secret.fromSsmParameter(props.schemaRegistryName),
                'BOOTSTRAP_SERVERS' : ecs.Secret.fromSsmParameter(props.bootstrapServersParameter)
            },
            cpu: 1024,
            memoryLimitMiB: 2048
        })

        const service = new ecs.FargateService(this, 'service', {
            cluster: props.ecsCluster,
            taskDefinition,
            securityGroups: [this.securityGroup]
        })

        this.taskRole.addToPolicy(new PolicyStatement({
            actions: [
                "kafka-cluster:Connect",
                "kafka-cluster:AlterCluster",
                "kafka-cluster:DescribeCluster"
            ],
            resources: [`arn:aws:kafka:${props.region}:${props.account}:cluster/${props.clusterName}/*`]
        }))

        this.taskRole.addToPolicy(new PolicyStatement({
            actions: [
                "kafka-cluster:*Topic*",
                "kafka-cluster:WriteData",
                "kafka-cluster:ReadData"
            ],
            resources: [`arn:aws:kafka:${props.region}:${props.account}:topic/${props.clusterName}/*`]
            //Topic scope can not be limited as user defined topics are allowed
        }))

        this.taskRole.addToPolicy(new PolicyStatement({
            actions: [
                "kafka-cluster:AlterGroup",
                "kafka-cluster:DescribeGroup"
            ],
            resources: [`arn:aws:kafka:${props.region}:${props.account}:group/${props.clusterName}/*`]
            //Group scope can not be limited as group name is random
        }))

        this.taskRole.addToPolicy(new PolicyStatement({
            actions: [
                'glue:GetSchemaVersion',
                "glue:CreateSchema",
                "glue:RegisterSchemaVersion",
                "glue:PutSchemaVersionMetadata",
                "glue:GetSchemaByDefinition"
            ],
            resources: [
                `arn:aws:glue:${props.region}:${props.account}:registry/${props.schemaRegistry.name}`,
                `arn:aws:glue:${props.region}:${props.account}:schema/*`
                //Topic scope can not be limited as user defined topics are allowed
            ]
        }))



    }
}