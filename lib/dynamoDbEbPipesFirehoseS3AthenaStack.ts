import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import { AthenaWorkGroup } from "./athenaWorkGroup";
import { DynamoDbPipeToFirehose } from "./dynamoDbPipeToFirehose";
import { FirehoseToS3 } from "./firehoseToS3";
import { GlueDbAndTables } from "./glueDbAndTables";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as lambda_node from "aws-cdk-lib/aws-lambda-nodejs";
import * as events from "aws-cdk-lib/aws-events";
import * as events_targets from "aws-cdk-lib/aws-events-targets";
import * as logs from "aws-cdk-lib/aws-logs";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as iam from "aws-cdk-lib/aws-iam";

export class DynamoDbEbPipesFirehoseS3AthenaStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    let firehoseLambdaBufferIntervalSeconds = 60; // minimum 60s, because partitioning is enabled, try to use larger, like 600s
    let lambdaProcessorBufferIntervalSeconds = 60; // minimum 60s, try to use larger, like 600s
    let bufferSizeMb = 3;

    // ****************************** DynamoDB tables **************************************
    const tableCutomerOrder = new dynamodb.TableV2(this, "CustomerOrder", {
      partitionKey: { name: "PK", type: dynamodb.AttributeType.STRING },
      sortKey: { name: "SK", type: dynamodb.AttributeType.STRING },
      dynamoStream: dynamodb.StreamViewType.NEW_AND_OLD_IMAGES,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const tableItem = new dynamodb.TableV2(this, "Item", {
      partitionKey: { name: "itemId", type: dynamodb.AttributeType.STRING },
      dynamoStream: dynamodb.StreamViewType.NEW_AND_OLD_IMAGES,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    // ************************* Glue schemas ************************
    const {
      databaseBucket,
      glueDb,
      glueTableAllColumns: glueTableAllColumnsForSigngleTable,
      glueTableCustomer,
      glueTableOrder,
      glueTableOrderItem,
      glueTableItem,
    } = new GlueDbAndTables(this, "GlueDb", {});

    // ************************* Athena workgroup **************************
    const athenaWorkGroup = new AthenaWorkGroup(this, "AthenaWorkGroup", {
      glueDatabaseName: glueDb.databaseName,
      databaseBucket,
    });

    // ************** DynamoDB -> EventBridge Pipe -> Firehose -> S3 -> Athena *****************
    const {
      firehoseDeliveryStream: firehoseDeliveryStreamItemCustomerOrderItem,
    } = new FirehoseToS3(this, "Firehose", {
      databaseBucket,
      glueTable: glueTableAllColumnsForSigngleTable,
      bufferSizeMb,
      lambdaProcessorBufferIntervalSeconds,
      firehoseLambdaBufferIntervalSeconds,
      // we use partitioning to store entities in single table design to multiple S3 folders
      usePartitioning: true,
      dynamicPartitioningByYearMonthDay: true,
    });

    const { firehoseDeliveryStream: firehoseDeliveryStreamItem } =
      new FirehoseToS3(this, "FirehosePartitioning", {
        databaseBucket,
        glueTable: glueTableItem,
        bufferSizeMb,
        lambdaProcessorBufferIntervalSeconds,
        firehoseLambdaBufferIntervalSeconds,
        usePartitioning: false,
        dynamicPartitioningByYearMonthDay: false,
        folderPrefix: "ITEM", // we do not use partitioning for this table, so we store all data in this folder
      });

    new DynamoDbPipeToFirehose(this, "PipeTableCustomerOrder", {
      table: tableCutomerOrder,
      firehoseDeliveryStream: firehoseDeliveryStreamItemCustomerOrderItem,
      // this table uses single table design, so we might want to filter by entity type
      entityTypes: ["ORDER", "CUSTOMER", "ORDER_ITEM"],
    });

    new DynamoDbPipeToFirehose(this, "PipeTableItem", {
      table: tableItem,
      firehoseDeliveryStream: firehoseDeliveryStreamItem,
      // this table does not use single table design, so no need to filter by entity type
      //entityTypes: ["ITEM"],
    });

    // ********************************* Functions **************************************
    const bundling: lambda_node.BundlingOptions = {
      format: lambda_node.OutputFormat.ESM,
      sourceMap: true,
      banner:
        'import { createRequire } from "module";const require = createRequire(import.meta.url);',
      tsconfig: "src/tsconfig.json",
    };

    const athenaQueryStartFunction = new lambda_node.NodejsFunction(
      this,
      "AthenaQueryStartFunc",
      {
        entry: "src/lambda/athena/athenaQueryStart.ts",
        runtime: lambda.Runtime.NODEJS_20_X,
        memorySize: 1024,
        timeout: cdk.Duration.seconds(10),
        logRetention: logs.RetentionDays.ONE_DAY,
        bundling,
        environment: {
          GLUE_TABLE_CUSTOMER_ORDER: glueTableCustomer.tableName,
          GLUE_TABLE_ORDER: glueTableOrder.tableName,
          GLUE_TABLE_ORDER_ITEM: glueTableOrderItem.tableName,
          GLUE_TABLE_CUSTOMER: glueTableCustomer.tableName,
          GLUE_ITEM_TABLE: glueTableItem.tableName,
          GLUE_DATABASE_NAME: glueDb.databaseName,
          ATHENA_WORK_GROUP_NAME: athenaWorkGroup.athenaWorkGroup.name,
          NODE_OPTIONS: "--enable-source-maps",

          //ATHENA_WORK_GROUP_NAME: athenaWorkGroup.workGroupName,
          //REPORT_TABLE_NAME: tableReport.tableName,
        },
      }
    );
    athenaWorkGroup.grantQueryExecution(athenaQueryStartFunction);

    const athenaQueryFinishedFunction = new lambda_node.NodejsFunction(
      this,
      "AthenaQueryFinishedFunc",
      {
        entry: "src/lambda/athena/athenaQueryFinished.ts",
        runtime: lambda.Runtime.NODEJS_20_X,
        memorySize: 1024,
        timeout: cdk.Duration.minutes(5),
        logRetention: logs.RetentionDays.ONE_DAY,
        bundling,
        environment: {
          NODE_OPTIONS: "--enable-source-maps",
        },
      }
    );
    athenaWorkGroup.grantReadQueryResults(athenaQueryFinishedFunction);
    // grand permissions glue:GetDatabase
    athenaQueryFinishedFunction.role?.attachInlinePolicy(
      new iam.Policy(this, "GlueGetDatabase", {
        statements: [
          new iam.PolicyStatement({
            actions: ["glue:GetDatabase"],
            resources: [glueDb.databaseArn],
          }),
        ],
      })
    );

    // ************************* Cron jobs **************************
    // Start Athena query once a day at 23:55 UTC
    //  > For real-world use cases, you might want to start after meednight.
    //  > Take into account time zones and buffer intervals, etc..
    //  > In that case you should also adjust queries to use yesterday's date.
    /* uncomment to enable
    new events.Rule(this, "AthenaQueryStartRule", {
      schedule: events.Schedule.cron({
        minute: "55",
        hour: "23",
      }),
      targets: [new events_targets.LambdaFunction(athenaQueryStartFunction)],
    });
    */

    // ************************** EventBridge ************************
    // subscribe to finished Athena query event
    new events.Rule(this, "AthenaQueryFinishedRule", {
      eventPattern: {
        source: ["aws.athena"],
        detailType: ["Athena Query State Change"],
        detail: {
          currentState: ["FAILED", "SUCCEEDED"],
        },
      },
      targets: [new events_targets.LambdaFunction(athenaQueryFinishedFunction)],
    });

    // ************************* Outputs **************************
    new cdk.CfnOutput(this, "DynamoDBTableCustomerOrder", {
      value: tableCutomerOrder.tableName,
    });

    new cdk.CfnOutput(this, "DynamoDBTableItem", {
      value: tableItem.tableName,
    });
  }
}
