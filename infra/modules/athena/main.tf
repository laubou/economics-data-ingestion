locals {
  name = "${var.project_name}-${var.environment}"
}

resource "aws_athena_workgroup" "main" {
  name        = local.name
  description = "Athena workgroup for querying bronze and silver Iceberg tables"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    engine_version {
      selected_engine_version = "Athena engine version 3"
    }
    result_configuration {
      output_location = "s3://${var.s3_bucket}/athena-results/"
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
    # Protect against runaway queries — adjust per environment
    bytes_scanned_cutoff_per_query = var.environment == "prod" ? null : 10737418240 # 10 GB in non-prod
  }

  tags = { Name = local.name }
}

# Named queries give analysts a quick-start for common patterns
resource "aws_athena_named_query" "bronze_preview" {
  name        = "bronze-preview"
  workgroup   = aws_athena_workgroup.main.id
  database    = var.glue_database
  description = "Preview the 100 most recent bronze records"
  query       = <<-SQL
    SELECT *
    FROM   sales_bronze
    ORDER  BY ingested_at DESC
    LIMIT  100;
  SQL
}

resource "aws_athena_named_query" "silver_by_country" {
  name        = "silver-revenue-by-country"
  workgroup   = aws_athena_workgroup.main.id
  database    = var.glue_database
  description = "Total revenue and margin by country for the latest month"
  query       = <<-SQL
    SELECT
        country,
        SUM(total_revenue)                             AS total_revenue,
        SUM(total_profit)                              AS total_profit,
        ROUND(AVG(margin_pct), 2)                      AS avg_margin_pct,
        SUM(units_sold)                                AS total_units
    FROM  sales_silver
    WHERE order_year  = YEAR(CURRENT_DATE)
      AND order_month = MONTH(CURRENT_DATE) - 1
    GROUP BY country
    ORDER BY total_revenue DESC;
  SQL
}

resource "aws_athena_named_query" "silver_dedup_check" {
  name        = "silver-dedup-check"
  workgroup   = aws_athena_workgroup.main.id
  database    = var.glue_database
  description = "Verify there are no duplicate source_kafka_offset values in silver"
  query       = <<-SQL
    SELECT source_kafka_offset, COUNT(*) AS cnt
    FROM   sales_silver
    GROUP  BY source_kafka_offset
    HAVING COUNT(*) > 1;
  SQL
}
