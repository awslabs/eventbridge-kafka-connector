/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

import {Construct} from "constructs";
import * as ec2 from "aws-cdk-lib/aws-ec2";
import * as logs from "aws-cdk-lib/aws-logs";
import {RetentionDays} from "aws-cdk-lib/aws-logs";
import * as ecs from "aws-cdk-lib/aws-ecs";
import {Compatibility, ContainerImage, LogDriver} from "aws-cdk-lib/aws-ecs";
import {PolicyStatement, Role, ServicePrincipal} from "aws-cdk-lib/aws-iam";
import * as path from "path";
import {aws_glue, aws_iam, RemovalPolicy} from "aws-cdk-lib";

export interface AnalyzerProps {
    ecsCluster: ecs.Cluster;
    vpc: ec2.Vpc;
    bootstrapServers: string;
    region: string;
    account: string;
    clusterName: string;
    schemaRegistry: aws_glue.CfnRegistry;
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
            environment: {
                BOOTSTRAP_SERVERS: props.bootstrapServers,
                TOPIC_NAME: 'events',
                NOTIFICATIONS_TOPIC_NAME: 'notifications',
                SCHEMA_REGISTRY_NAME : props.schemaRegistry.name
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