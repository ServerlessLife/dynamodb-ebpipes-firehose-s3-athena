import * as constructs from "constructs";
import * as iam from "aws-cdk-lib/aws-iam";
import type * as s3 from "aws-cdk-lib/aws-s3";
import * as firehose from "aws-cdk-lib/aws-kinesisfirehose";
import type * as glueA from "@aws-cdk/aws-glue-alpha";
import * as logs from "aws-cdk-lib/aws-logs";
import * as cdk from "aws-cdk-lib";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as lambda_node from "aws-cdk-lib/aws-lambda-nodejs";

export interface FirehoseToS3Props {
  readonly databaseBucket: s3.Bucket;
  readonly glueTable: glueA.S3Table;
  readonly bufferSizeMb: number;
  readonly lambdaProcessorBufferIntervalSeconds: number;
  readonly firehoseLambdaBufferIntervalSeconds: number;
  readonly usePartitioning: boolean;
  readonly dynamicPartitioningByYearMonthDay: boolean;
  readonly folderPrefix?: string;
}
export class FirehoseToS3 extends constructs.Construct {
  firehoseDeliveryStream: firehose.CfnDeliveryStream;
  firehoseLambdaTransformation: lambda.Function;

  constructor(
    scope: constructs.Construct,
    id: string,
    props: FirehoseToS3Props
  ) {
    super(scope, id);

    this.firehoseLambdaTransformation = new lambda_node.NodejsFunction(
      this,
      "LambdaTransformationFunc",
      {
        entry: "src/lambda/firehose/firehoseLambdaTransformation.ts",
        memorySize: 1024,
        timeout: cdk.Duration.minutes(5),
        logRetention: logs.RetentionDays.ONE_DAY,
        bundling: {
          format: lambda_node.OutputFormat.ESM,
          sourceMap: true,
          banner:
            'import { createRequire } from "module";const require = createRequire(import.meta.url);',
          tsconfig: "src/tsconfig.json",
        },
        environment: {
          NODE_OPTIONS: "--enable-source-maps",
        },
      }
    );

    const logGroup = new logs.LogGroup(this, "FirehoseS3Log", {
      retention: logs.RetentionDays.ONE_DAY,
    });

    // https://5k-team.trilogy.com/hc/en-us/articles/360015651640-Configuring-Firehose-with-CDK
    const deliveryStreamRole = new iam.Role(this, "DeliveryStreamRole", {
      assumedBy: new iam.ServicePrincipal("firehose.amazonaws.com"),
      inlinePolicies: {
        kinesisS3Policy: iam.PolicyDocument.fromJson({
          Version: "2012-10-17",
          Statement: [
            {
              Effect: "Allow",
              Action: [
                "s3:AbortMultipartUpload",
                "s3:GetBucketLocation",
                "s3:Abort*",
                "s3:DeleteObject*",
                "s3:GetBucket*",
                "s3:GetObject*",
                "s3:List*",
                "s3:PutObject",
                "s3:PutObjectLegalHold",
                "s3:PutObjectRetention",
                "s3:PutObjectTagging",
                "s3:PutObjectVersionTagging",
              ],
              Resource: [
                props.databaseBucket.bucketArn,
                `${props.databaseBucket.bucketArn}/*`,
              ],
            },
          ],
        }),
        cloudwatch: iam.PolicyDocument.fromJson({
          Version: "2012-10-17",
          Statement: [
            {
              Effect: "Allow",
              Action: [
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents",
              ],
              Resource: [logGroup.logGroupArn],
            },
          ],
        }),
        kinesisLambdaPolicy: iam.PolicyDocument.fromJson({
          Version: "2012-10-17",
          Statement: [
            {
              Effect: "Allow",
              Action: [
                "lambda:InvokeFunction",
                "lambda:GetFunctionConfiguration",
              ],
              Resource: this.firehoseLambdaTransformation.functionArn,
            },
          ],
        }),
        gluePolicy: iam.PolicyDocument.fromJson({
          Version: "2012-10-17",
          Statement: [
            {
              Effect: "Allow",
              Action: ["glue:GetTableVersions"],
              Resource: "*",
            },
          ],
        }),
      },
    });

    // Provides Put, Get access to S3 and full access to CloudWatch Logs.
    deliveryStreamRole.addManagedPolicy({
      managedPolicyArn: "arn:aws:iam::aws:policy/AWSLambdaExecute",
    });

    const lambdaProcessor = {
      type: "Lambda",
      parameters: [
        {
          parameterName: "LambdaArn",
          parameterValue: this.firehoseLambdaTransformation.functionArn,
        },
        {
          parameterName: "NumberOfRetries",
          parameterValue: "3",
        },
        {
          parameterName: "RoleArn",
          parameterValue: deliveryStreamRole.roleArn,
        },
        {
          parameterName: "BufferSizeInMBs",
          parameterValue: props.bufferSizeMb.toString(),
        },
        {
          parameterName: "BufferIntervalInSeconds",
          parameterValue: props.lambdaProcessorBufferIntervalSeconds.toString(),
        },
      ],
    };

    const metadataExtractionProcessor = {
      type: "MetadataExtraction",
      parameters: [
        {
          parameterName: "MetadataExtractionQuery",
          parameterValue:
            "{entity_type: .entity_type, year: .year, month: .month, day: .day}",
        },
        {
          parameterName: "JsonParsingEngine",
          parameterValue: "JQ-1.6",
        },
      ],
    };

    const deliveryStreamProps: firehose.CfnDeliveryStreamProps = {
      deliveryStreamType: "DirectPut",
      extendedS3DestinationConfiguration: {
        cloudWatchLoggingOptions: {
          enabled: true,
          logGroupName: logGroup.logGroupName,
          logStreamName: "FirehoseS3Log",
        },
        bucketArn: props.databaseBucket.bucketArn,
        roleArn: deliveryStreamRole.roleArn,
        bufferingHints: {
          sizeInMBs: 128,
          intervalInSeconds: props.firehoseLambdaBufferIntervalSeconds,
        },
        prefix: getPrefix(props),
        errorOutputPrefix:
          "errors/!{firehose:error-output-type}/!{timestamp:yyyy/MM/dd}/",
        compressionFormat: "UNCOMPRESSED", // we are using parquet which is already compressed
        dataFormatConversionConfiguration: {
          enabled: true,
          schemaConfiguration: {
            databaseName: props.glueTable.database.databaseName,
            tableName: props.glueTable.tableName,
            roleArn: deliveryStreamRole.roleArn,
          },
          inputFormatConfiguration: {
            deserializer: {
              openXJsonSerDe: {
                caseInsensitive: false,
                columnToJsonKeyMappings: {},
                convertDotsInJsonKeysToUnderscores: false,
              },
            },
          },
          outputFormatConfiguration: {
            serializer: {
              parquetSerDe: {
                compression: "SNAPPY",
              },
            },
          },
        },
        dynamicPartitioningConfiguration:
          props.usePartitioning || props.dynamicPartitioningByYearMonthDay
            ? {
                enabled: props.dynamicPartitioningByYearMonthDay,
              }
            : undefined,
        processingConfiguration: {
          enabled: true,
          processors:
            props.usePartitioning || props.dynamicPartitioningByYearMonthDay
              ? [metadataExtractionProcessor, lambdaProcessor]
              : [lambdaProcessor],
        },
      },
    };

    this.firehoseDeliveryStream = new firehose.CfnDeliveryStream(
      this,
      "FirehoseDeliveryStream",
      deliveryStreamProps
    );
  }
}

function getPrefix(props: FirehoseToS3Props): string | undefined {
  let prefix: string = "";

  if (props.usePartitioning) {
    prefix = "!{partitionKeyFromQuery:entity_type}/";
  }

  if (props.dynamicPartitioningByYearMonthDay) {
    // use time from the record
    prefix = `${prefix}year=!{partitionKeyFromQuery:year}/month=!{partitionKeyFromQuery:month}/day=!{partitionKeyFromQuery:day}/`;
  } else {
    // use current time
    prefix = `${prefix}year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/`;
  }

  if (props.folderPrefix) {
    prefix = `${props.folderPrefix}/${prefix}`;
  }

  return prefix;
}
