#!/usr/bin/env node
import "source-map-support/register";
import * as cdk from "aws-cdk-lib";
import { DynamoDbEbPipesFirehoseS3AthenaStack } from "../lib/dynamoDbEbPipesFirehoseS3AthenaStack";

const app = new cdk.App();
new DynamoDbEbPipesFirehoseS3AthenaStack(app, "DynamoDbAthena", {
  stackName: "dynamodb-pipes-athena",
});
