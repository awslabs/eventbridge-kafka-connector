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
import {aws_glue, aws_iam} from "aws-cdk-lib";
import {NagSuppressions} from "cdk-nag";

export interface ProducerProps {
    vpc: ec2.Vpc;
    bootstrapServers: string;
    region: string;
    account: string;
    clusterName: string;
    schemaRegistry: aws_glue.CfnRegistry;
}

export class Producer extends Construct {

    public readonly securityGroup: ec2.SecurityGroup;
    public readonly taskRole: Role;
    public readonly ecsCluster: ecs.Cluster;


    constructor(scope: Construct, id: string, props: ProducerProps) {
        super(scope, id);

        const cluster = new ecs.Cluster(this, "cluster", {
            vpc: props.vpc
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
            environment: {
                BOOTSTRAP_SERVERS: props.bootstrapServers,
                TOPIC_NAME: 'events',
                NUMBER_OF_PRODUCERS: '1',
                SCHEMA_REGISTRY_NAME : props.schemaRegistry.name
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

        NagSuppressions.addResourceSuppressions(cluster, [
            {
                id: 'AwsSolutions-ECS4',
                reason: 'Not needed for this sample, keeping cost low'
            }
        ])

    }
}