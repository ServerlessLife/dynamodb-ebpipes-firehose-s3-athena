import {
  type FirehoseTransformationHandler,
  type FirehoseTransformationResult,
} from "aws-lambda";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import { EntityType } from "../../types/entityType";

export const handler: FirehoseTransformationHandler = async (event) => {
  const output = event.records.map((record) => {
    const dataString = Buffer.from(record.data, "base64").toString();
    const data = JSON.parse(dataString);

    try {
      // unmarshall converts DynamoDB JSON to normal JSON
      const dynamoDbData = unmarshall(
        data.dynamodb.NewImage ?? data.dynamodb.OldImage
      );

      // add deleted flag if there is no NewImage and there is only OldImage
      if (!data.dynamodb.NewImage && data.dynamodb.OldImage) {
        dynamoDbData.deleted = true;
      }

      // single table design where entity type defines S3 folder using Firehose partitioning
      const entityType = dynamoDbData.ENTITY_TYPE as EntityType | undefined;

      if (entityType) {
        delete dynamoDbData.ENTITY_TYPE;
        dynamoDbData.entity_type = entityType;
      }

      // partitioning by year, month, day
      if (dynamoDbData.date) {
        // Athena partitioning by year, month, day
        dynamoDbData.year = parseInt(dynamoDbData.date.substring(0, 4));
        dynamoDbData.month = parseInt(dynamoDbData.date.substring(5, 7));
        dynamoDbData.day = parseInt(dynamoDbData.date.substring(8, 10));
      }

      const transformed: any = {};

      // change camel case to snake_case
      for (const key in dynamoDbData) {
        let newKey: string;
        if (key === "PK" || key === "SK" || key.startsWith("GSI")) {
          // do not save partition and sort keys
          continue;
        } else {
          // Glue does not support camel case
          newKey = camelToSnakeCase(key);
        }

        transformed[newKey] = dynamoDbData[key];
      }

      const transformedString = JSON.stringify(transformed);

      console.log("Transformed", transformedString);

      return {
        recordId: record.recordId,
        result: "Ok",
        data: Buffer.from(transformedString).toString("base64"),
      };
    } catch (e) {
      console.log("ERROR", e);
      return {
        recordId: record.recordId,
        result: "ProcessingFailed",
        data: record.data,
      };
    }
  });

  return { records: output } as FirehoseTransformationResult;
};

function camelToSnakeCase(key: string) {
  return key.replace(/([A-Z])/g, "_$1").toLowerCase();
}
