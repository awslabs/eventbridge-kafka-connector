/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

import {Construct} from "constructs";
import * as ec2 from "aws-cdk-lib/aws-ec2";
import * as ecs from "aws-cdk-lib/aws-ecs";
import {Compatibility, ContainerImage, LogDriver} from "aws-cdk-lib/aws-ecs";
import {PolicyStatement, Role, ServicePrincipal} from "aws-cdk-lib/aws-iam";
import * as path from "path";
import {aws_glue, aws_iam, aws_ssm} from "aws-cdk-lib";
import {NagSuppressions} from "cdk-nag";

export interface ProducerProps {
    vpc: ec2.Vpc;
    bootstrapServersParameter: aws_ssm.StringParameter;
    region: string;
    account: string;
    clusterName: string;
    schemaRegistry: aws_glue.CfnRegistry;
    schemaRegistryName: aws_ssm.StringParameter;
    eventsTopic: aws_ssm.StringParameter;
    notificationTopic: aws_ssm.StringParameter;
}

export class Producer extends Construct {

    public readonly securityGroup: ec2.SecurityGroup;
    public readonly taskRole: Role;
    public readonly ecsCluster: ecs.Cluster;


    constructor(scope: Construct, id: string, props: ProducerProps) {
        super(scope, id);

        const cluster = new ecs.Cluster(this, "cluster", {
            vpc: props.vpc,
            containerInsights: true
        });

        this.ecsCluster = cluster

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
            image: ContainerImage.fromAsset(path.join(__dirname, '../kafkaProducer')),
            logging: LogDriver.awsLogs({
                streamPrefix: 'kafkaProducer',
            }),
            secrets: {
                'BOOTSTRAP_SERVERS' : ecs.Secret.fromSsmParameter(props.bootstrapServersParameter),
                'TOPIC_NAME': ecs.Secret.fromSsmParameter(props.eventsTopic),
                'SCHEMA_REGISTRY_NAME': ecs.Secret.fromSsmParameter(props.schemaRegistryName)
            },
            cpu: 1024,
            memoryLimitMiB: 2048
        })

        const service = new ecs.FargateService(this, 'service', {
            cluster,
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
        }))

        this.taskRole.addToPolicy(new PolicyStatement({
            actions: [
                "kafka-cluster:AlterGroup",
                "kafka-cluster:DescribeGroup"
            ],
            resources: [`arn:aws:kafka:${props.region}:${props.account}:group/${props.clusterName}/*`]
        }))

        this.taskRole.addToPolicy(new PolicyStatement({
            actions: [
                'glue:GetSchemaVersion',
                "glue:CreateSchema",
                "glue:RegisterSchemaVersion",
                "glue:PutSchemaVersionMetadata",
                "glue:GetSchemaByDefinition"
            ],
            resources: ['*']
        }))

    }
}