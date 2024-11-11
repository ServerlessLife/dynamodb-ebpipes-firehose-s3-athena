import * as constructs from "constructs";
import * as iam from "aws-cdk-lib/aws-iam";
import * as pipe from "aws-cdk-lib/aws-pipes";
import type * as firehose from "aws-cdk-lib/aws-kinesisfirehose";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as dynamoDb from "aws-cdk-lib/aws-dynamodb";

export interface DynamoDbPipeToFirehoseProps {
  readonly table: dynamoDb.ITable;
  readonly firehoseDeliveryStream: firehose.CfnDeliveryStream;
  readonly entityTypes?: string[];
}

export class DynamoDbPipeToFirehose extends constructs.Construct {
  pipe: pipe.CfnPipe;

  constructor(
    scope: constructs.Construct,
    id: string,
    props: DynamoDbPipeToFirehoseProps
  ) {
    super(scope, id);

    let filterCriteria: pipe.CfnPipe.FilterCriteriaProperty | undefined;

    if (props.entityTypes?.length) {
      filterCriteria = {
        filters: [
          {
            pattern: JSON.stringify({
              // filter by INSERT if you want to capture only new items in case you have append only design
              //eventName: ["INSERT", "MODIFY", "REMOVE"],

              // filter by entity type for single table design. Entity type is stored in property ENTITY_TYPE
              dynamodb: {
                NewImage: {
                  ENTITY_TYPE: {
                    S: props.entityTypes,
                  },
                },
              },
            }),
          },
        ],
      };
    }

    const pipeDlq = new sqs.Queue(this, "PipeDlq");

    const pipeTableAppRole = new iam.Role(this, "PipeTableAppRole", {
      assumedBy: new iam.ServicePrincipal("pipes.amazonaws.com"),
      inlinePolicies: {
        sourcePolicy: new iam.PolicyDocument({
          statements: [
            new iam.PolicyStatement({
              resources: [props.table.tableStreamArn!],
              actions: [
                "dynamodb:DescribeStream",
                "dynamodb:GetRecords",
                "dynamodb:GetShardIterator",
                "dynamodb:ListStreams",
              ],
              effect: iam.Effect.ALLOW,
            }),
          ],
        }),
        targetPolicy: new iam.PolicyDocument({
          statements: [
            new iam.PolicyStatement({
              resources: [props.firehoseDeliveryStream.attrArn],
              actions: ["firehose:PutRecord", "firehose:PutRecordBatch"],
              effect: iam.Effect.ALLOW,
            }),
          ],
        }),
        dlqPolicy: new iam.PolicyDocument({
          statements: [
            new iam.PolicyStatement({
              resources: [pipeDlq.queueArn],
              actions: ["sqs:SendMessage"],
              effect: iam.Effect.ALLOW,
            }),
          ],
        }),
      },
    });

    this.pipe = new pipe.CfnPipe(this, "PipeTableApp", {
      roleArn: pipeTableAppRole.roleArn,
      source: props.table.tableStreamArn!,
      sourceParameters: {
        dynamoDbStreamParameters: {
          startingPosition: "LATEST",
          deadLetterConfig: {
            arn: pipeDlq.queueArn,
          },
        },
        filterCriteria: filterCriteria,
      },
      target: props.firehoseDeliveryStream.attrArn,
    });
  }
}
