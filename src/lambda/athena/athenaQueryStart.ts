import {
  AthenaClient,
  StartQueryExecutionCommand,
} from "@aws-sdk/client-athena";
import { Handler } from "aws-lambda";

const athena = new AthenaClient({});

export const handler: Handler = async () => {
  const now = new Date();

  // Total earnings each day
  await executeQuery(
    `
    WITH "order" AS (
      SELECT DISTINCT *
      FROM "${process.env.GLUE_TABLE_ORDER}"
    ),
    "order_item" AS (
      SELECT DISTINCT *
      FROM "${process.env.GLUE_TABLE_ORDER_ITEM}"
    )
    SELECT year, month, day, SUM(oi.price * oi.quantity) AS total
      FROM "order" AS o
          INNER JOIN "order_item" AS oi
                  ON oi.order_id = o.order_id
    WHERE year = ?
      AND month = ?
      AND day = ?
    GROUP BY year, month, day
   `,
    [
      now.getFullYear().toString(),
      (now.getMonth() + 1).toString(),
      now.getDate().toString(),
    ]
  );

  // Most expensive order of the day with all ordered items
  await executeQuery(
    `
    WITH "order" AS (
      SELECT DISTINCT *
      FROM "${process.env.GLUE_TABLE_ORDER}"
    ),
    "order_item" AS (
      SELECT DISTINCT *
      FROM "${process.env.GLUE_TABLE_ORDER_ITEM}"
    ),
    "customer" AS (
      SELECT DISTINCT *
      FROM "${process.env.GLUE_TABLE_CUSTOMER}"
    )
    SELECT o.order_id,
           c.customer_id,
           c.name AS customer_name,
           SUM(oi.price * oi.quantity) AS total,
           ARRAY_AGG(
             CAST(
               CAST(
                 ROW(oi.item_id, oi.item_name, oi.quantity, oi.price)
                   AS ROW(item_id VARCHAR, item_name VARCHAR, quantity INTEGER, price DOUBLE)
             ) AS JSON)
           ) AS items
      FROM "order" AS o
          INNER JOIN "order_item" AS oi
                  ON oi.order_id = o.order_id
          INNER JOIN "customer" AS c
                  ON c.customer_id = o.customer_id
    WHERE year = ?
      AND month = ?
      AND day = ?
    GROUP BY o.order_id, c.customer_id, c.name
    ORDER BY total DESC
    LIMIT 1
   `,
    [
      now.getFullYear().toString(),
      (now.getMonth() + 1).toString(),
      now.getDate().toString(),
    ]
  );

  // Total inventory count
  await executeQuery(
    `
    SELECT COUNT(DISTINCT i.item_id) AS total_items
      FROM "${process.env.GLUE_ITEM_TABLE}" AS i
           LEFT OUTER JOIN (SELECT DISTINCT item_id
                              FROM "${process.env.GLUE_ITEM_TABLE}"
                             WHERE deleted = true) AS i_deleted
                    ON i_deleted.item_id = i.item_id
    WHERE i_deleted.item_id IS NULL
   `
  );
};

async function executeQuery(query: string, parameters?: string[]) {
  const command = new StartQueryExecutionCommand({
    QueryString: query,
    QueryExecutionContext: {
      Database: process.env.GLUE_DATABASE_NAME,
    },
    WorkGroup: process.env.ATHENA_WORK_GROUP_NAME,
    ExecutionParameters: parameters,
  });

  const { QueryExecutionId } = await athena.send(command);
  console.log(`Query started with ID: ${QueryExecutionId}`);
}
