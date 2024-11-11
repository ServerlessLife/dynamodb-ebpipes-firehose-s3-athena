import * as constructs from "constructs";
import * as cdk from "aws-cdk-lib";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as glueA from "@aws-cdk/aws-glue-alpha";
import * as glue from "aws-cdk-lib/aws-glue";

export interface GlueDbAndTablesProps {}

export class GlueDbAndTables extends constructs.Construct {
  public readonly databaseBucket: s3.Bucket;
  public readonly glueDb: glueA.Database;
  public readonly glueTableAllColumns: glueA.S3Table;
  public readonly glueTableCustomer: glueA.S3Table;
  public readonly glueTableOrder: glueA.S3Table;
  public readonly glueTableOrderItem: glueA.S3Table;
  public readonly glueTableItem: glueA.S3Table;

  constructor(
    scope: constructs.Construct,
    id: string,
    props: GlueDbAndTablesProps
  ) {
    super(scope, id);

    this.databaseBucket = new s3.Bucket(this, "DatabaseBucket", {
      encryption: s3.BucketEncryption.S3_MANAGED,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    this.glueDb = new glueA.Database(this, "GlueDatabase", {
      databaseName: `${cdk.Stack.of(
        this
      ).stackName.toLocaleLowerCase()}-database`,
    });

    /******************Glue tables for "CustomerOrder" DynamoDb table******************/
    // note: Single table design is used for CustomerOrder table
    this.glueTableCustomer = new glueA.S3Table(this, "GlueTableCustomer", {
      bucket: this.databaseBucket,
      s3Prefix: "CUSTOMER/",
      columns: [
        { name: "customer_id", type: glueA.Schema.STRING },
        { name: "name", type: glueA.Schema.STRING },
        { name: "email", type: glueA.Schema.STRING },
      ],
      dataFormat: glueA.DataFormat.PARQUET,
      database: this.glueDb,
      tableName: `${cdk.Stack.of(this).stackName.toLocaleLowerCase()}-customer`,
    });

    this.glueTableOrder = new glueA.S3Table(this, "GlueTableOrder", {
      bucket: this.databaseBucket,
      s3Prefix: "ORDER/",
      columns: [
        { name: "order_id", type: glueA.Schema.STRING },
        { name: "customer_id", type: glueA.Schema.STRING },
        { name: "date", type: glueA.Schema.TIMESTAMP },
      ],
      dataFormat: glueA.DataFormat.PARQUET,
      database: this.glueDb,
      tableName: `${cdk.Stack.of(this).stackName.toLocaleLowerCase()}-order`,
      partitionKeys: [
        { name: "year", type: glueA.Schema.INTEGER },
        { name: "month", type: glueA.Schema.INTEGER },
        { name: "day", type: glueA.Schema.INTEGER },
      ],
    });

    (this.glueTableOrder.node.defaultChild as glue.CfnTable).addOverride(
      "Properties.TableInput.Parameters",
      {
        "projection.enabled": "true",
        "projection.year.type": "date",
        "projection.year.format": "yyyy",
        "projection.year.range": "2022,NOW",
        "projection.year.interval": "1",
        "projection.year.unit": "YEARS",
        "projection.month.type": "integer",
        "projection.month.range": "01,12",
        "projection.month.digits": "2",
        "projection.day.type": "integer",
        "projection.day.range": "01,31",
        "projection.day.digits": "2",
      }
    );

    this.glueTableOrderItem = new glueA.S3Table(this, "GlueTableOrderItem", {
      bucket: this.databaseBucket,
      s3Prefix: "ORDER_ITEM/",
      columns: [
        { name: "order_id", type: glueA.Schema.STRING },
        { name: "item_id", type: glueA.Schema.STRING },
        { name: "item_name", type: glueA.Schema.STRING },
        { name: "price", type: glueA.Schema.DOUBLE }, // do not use double for money
        { name: "quantity", type: glueA.Schema.INTEGER },
      ],
      dataFormat: glueA.DataFormat.PARQUET,
      database: this.glueDb,
      tableName: `${cdk.Stack.of(
        this
      ).stackName.toLocaleLowerCase()}-order-item`,
    });

    // for table CustomerOrder that uses single table design I need glue table that contains all columns
    const allColumns = [
      ...this.glueTableCustomer.columns,
      ...this.glueTableOrder.columns,
      ...this.glueTableOrderItem.columns,
      { name: "entity_type", type: glueA.Schema.STRING },
    ];
    const uniqueColumns = allColumns.filter(
      (thing, index, self) =>
        index === self.findIndex((t) => t.name === thing.name)
    );

    this.glueTableAllColumns = new glueA.S3Table(this, "GlueTableAllColumns", {
      bucket: this.databaseBucket,
      columns: uniqueColumns,
      dataFormat: glueA.DataFormat.PARQUET,
      database: this.glueDb,
      tableName: `${cdk.Stack.of(
        this
      ).stackName.toLocaleLowerCase()}-all-columns`,
    });

    /******************Glue table for "Item" DynamoDb table******************/
    this.glueTableItem = new glueA.S3Table(this, "GlueTableItem", {
      bucket: this.databaseBucket,
      s3Prefix: "ITEM/",
      columns: [
        { name: "item_id", type: glueA.Schema.STRING },
        { name: "name", type: glueA.Schema.STRING },
        { name: "price", type: glueA.Schema.DOUBLE }, // do not use double for money
        { name: "category", type: glueA.Schema.STRING },
        { name: "deleted", type: glueA.Schema.BOOLEAN },
      ],
      dataFormat: glueA.DataFormat.PARQUET,
      database: this.glueDb,
      tableName: `${cdk.Stack.of(this).stackName.toLocaleLowerCase()}-item`,
    });
  }
}
