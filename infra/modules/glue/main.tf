locals {
  name = "${var.project_name}_${var.environment}"
  # Underscores because Glue database names cannot contain hyphens
}

# ---- Glue Database ----

resource "aws_glue_catalog_database" "main" {
  name        = local.name
  description = "Economics pipeline — bronze and silver Iceberg tables"
}

# ---- IAM role for Glue crawlers / jobs ----

data "aws_iam_policy_document" "glue_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue" {
  name               = "${var.project_name}-${var.environment}-glue"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_s3" {
  name = "glue-s3-access"
  role = aws_iam_role.glue.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
      Resource = [var.s3_bucket_arn, "${var.s3_bucket_arn}/*"]
    }]
  })
}

# ---- Glue tables are registered by PyIceberg at runtime, not via Terraform ----
# The DDL equivalent (for documentation / Athena queries) is:
#
#   CREATE TABLE <db>.sales_bronze (
#     order_id        BIGINT,
#     region          STRING,
#     country         STRING,
#     item_type       STRING,
#     sales_channel   STRING,
#     priority        STRING,
#     order_date      DATE,
#     ship_date       DATE,
#     units_sold      INT,
#     unit_price      DOUBLE,
#     unit_cost       DOUBLE,
#     total_revenue   DOUBLE,
#     total_cost      DOUBLE,
#     total_profit    DOUBLE,
#     kafka_topic     STRING,
#     kafka_partition INT,
#     kafka_offset    BIGINT,
#     ingested_at     TIMESTAMP,
#     source_file     STRING
#   )
#   LOCATION 's3://<bucket>/bronze/'
#   TBLPROPERTIES ('table_type' = 'ICEBERG', 'format' = 'parquet');
#
#   CREATE TABLE <db>.sales_silver (
#     order_id                BIGINT,
#     region                  STRING,
#     country                 STRING,
#     item_type               STRING,
#     sales_channel           STRING,
#     priority                STRING,
#     order_date              DATE,
#     ship_date               DATE,
#     order_year              INT,
#     order_month             INT,
#     lead_time_days          INT,
#     units_sold              INT,
#     unit_price              DECIMAL(18,4),
#     unit_cost               DECIMAL(18,4),
#     total_revenue           DECIMAL(18,4),
#     total_cost              DECIMAL(18,4),
#     total_profit            DECIMAL(18,4),
#     margin_pct              DECIMAL(6,2),
#     bronze_ingested_at      TIMESTAMP,
#     silver_transformed_at   TIMESTAMP,
#     source_kafka_offset     STRING
#   )
#   PARTITIONED BY (order_year, order_month)
#   LOCATION 's3://<bucket>/silver/'
#   TBLPROPERTIES ('table_type' = 'ICEBERG', 'format' = 'parquet');
