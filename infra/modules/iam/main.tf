locals {
  name = "${var.project_name}-${var.environment}"
}

# ------------------------------------------------------------------ #
# Pipeline service role — attached to ECS tasks / Lambda / Glue jobs  #
# ------------------------------------------------------------------ #

data "aws_iam_policy_document" "pipeline_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com", "lambda.amazonaws.com", "glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "pipeline" {
  name               = "${local.name}-pipeline"
  assume_role_policy = data.aws_iam_policy_document.pipeline_assume.json
}

# ---- S3 permissions ----

resource "aws_iam_role_policy" "s3" {
  name = "s3-lake-access"
  role = aws_iam_role.pipeline.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ListBucket"
        Effect = "Allow"
        Action = ["s3:ListBucket", "s3:GetBucketLocation"]
        Resource = [var.s3_bucket_arn]
      },
      {
        Sid    = "ReadWrite"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:AbortMultipartUpload",
          "s3:ListMultipartUploadParts"
        ]
        Resource = ["${var.s3_bucket_arn}/*"]
      }
    ]
  })
}

# ---- MSK permissions ----

resource "aws_iam_role_policy" "msk" {
  name = "msk-access"
  role = aws_iam_role.pipeline.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "kafka-cluster:Connect",
        "kafka-cluster:DescribeCluster",
        "kafka-cluster:AlterCluster",
        "kafka-cluster:DescribeTopic",
        "kafka-cluster:CreateTopic",
        "kafka-cluster:WriteData",
        "kafka-cluster:ReadData",
        "kafka-cluster:DescribeGroup",
        "kafka-cluster:AlterGroup"
      ]
      Resource = [
        var.msk_arn,
        "${var.msk_arn}/topic/*",
        "${var.msk_arn}/group/*"
      ]
    }]
  })
}

# ---- Glue Data Catalog permissions ----

resource "aws_iam_role_policy" "glue_catalog" {
  name = "glue-catalog-access"
  role = aws_iam_role.pipeline.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "glue:GetDatabase",
        "glue:GetTable",
        "glue:GetTables",
        "glue:CreateTable",
        "glue:UpdateTable",
        "glue:DeleteTable",
        "glue:GetPartition",
        "glue:GetPartitions",
        "glue:BatchCreatePartition",
        "glue:BatchDeletePartition"
      ]
      Resource = [
        "arn:aws:glue:${var.aws_region}:${var.account_id}:catalog",
        "arn:aws:glue:${var.aws_region}:${var.account_id}:database/${var.glue_database}",
        "arn:aws:glue:${var.aws_region}:${var.account_id}:table/${var.glue_database}/*"
      ]
    }]
  })
}

# ---- CloudWatch Logs (for ECS task logs) ----

resource "aws_iam_role_policy_attachment" "cloudwatch" {
  role       = aws_iam_role.pipeline.name
  policy_arn = "arn:aws:iam::aws:policy/CloudWatchLogsFullAccess"
}
