###############################################################################
# Scheduler module — daily batch ingestion trigger
#
# Architecture:
#   EventBridge Scheduler (cron 09:00 UTC)
#       └─► Step Functions state machine
#               ├─► ECS Fargate task: downloader  (fetch → archive/ → landing/)
#               └─► ECS Fargate task: producer    (landing/ → Kafka topic)
#                   (only runs if downloader exited 0)
#
# The Step Functions machine enforces the sequential dependency between
# downloader and producer. If the downloader finds no new data it exits 0
# but the ingestion state file stays at "produced" — producer is still
# triggered but immediately skips (idempotent).
#
# consumer_bronze and transformer_silver are ALWAYS-ON ECS services
# managed by the ECS module, not triggered here.
###############################################################################

locals {
  prefix = "${var.environment}-economics-pipeline"
}

# ─── IAM: EventBridge → Step Functions ──────────────────────────────────────

resource "aws_iam_role" "eventbridge_scheduler" {
  name = "${local.prefix}-scheduler-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "scheduler.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "eventbridge_start_sfn" {
  name = "start-state-machine"
  role = aws_iam_role.eventbridge_scheduler.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "states:StartExecution"
      Resource = aws_sfn_state_machine.ingestion.arn
    }]
  })
}

# ─── IAM: Step Functions → ECS ──────────────────────────────────────────────

resource "aws_iam_role" "sfn_execution" {
  name = "${local.prefix}-sfn-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "states.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "sfn_run_ecs" {
  name = "run-ecs-tasks"
  role = aws_iam_role.sfn_execution.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "ecs:RunTask",
          "ecs:StopTask",
          "ecs:DescribeTasks",
        ]
        Resource = "*"
      },
      {
        # Step Functions needs to pass the task execution role to ECS
        Effect   = "Allow"
        Action   = "iam:PassRole"
        Resource = "*"
        Condition = {
          StringLike = {
            "iam:PassedToService" = "ecs-tasks.amazonaws.com"
          }
        }
      },
      {
        # Required for .sync:2 integration (wait for task completion)
        Effect = "Allow"
        Action = [
          "events:PutTargets",
          "events:PutRule",
          "events:DescribeRule",
        ]
        Resource = "arn:aws:events:${var.aws_region}:*:rule/StepFunctionsGetEventsForECSTaskRule"
      },
    ]
  })
}

# ─── Step Functions: downloader → producer ───────────────────────────────────

resource "aws_sfn_state_machine" "ingestion" {
  name     = "${local.prefix}-ingestion"
  role_arn = aws_iam_role.sfn_execution.arn

  # Uses the optimised .sync:2 integration which waits for ECS task completion
  # and propagates the exit code before moving to the next state.
  definition = jsonencode({
    Comment = "Daily batch ingestion: downloader then producer"
    StartAt = "Downloader"
    States = {
      Downloader = {
        Type     = "Task"
        Resource = "arn:aws:states:::ecs:runTask.sync:2"
        Parameters = {
          Cluster        = var.ecs_cluster_arn
          TaskDefinition = var.downloader_task_definition_arn
          LaunchType     = "FARGATE"
          NetworkConfiguration = {
            AwsvpcConfiguration = {
              Subnets        = var.private_subnet_ids
              SecurityGroups = [var.pipeline_security_group_id]
              AssignPublicIp = "DISABLED"
            }
          }
        }
        Next  = "Producer"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "IngestionFailed"
          ResultPath  = "$.error"
        }]
      }

      Producer = {
        Type     = "Task"
        Resource = "arn:aws:states:::ecs:runTask.sync:2"
        Parameters = {
          Cluster        = var.ecs_cluster_arn
          TaskDefinition = var.producer_task_definition_arn
          LaunchType     = "FARGATE"
          NetworkConfiguration = {
            AwsvpcConfiguration = {
              Subnets        = var.private_subnet_ids
              SecurityGroups = [var.pipeline_security_group_id]
              AssignPublicIp = "DISABLED"
            }
          }
        }
        End   = true
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "IngestionFailed"
          ResultPath  = "$.error"
        }]
      }

      IngestionFailed = {
        Type  = "Fail"
        Error = "IngestionCycleFailed"
        Cause = "A pipeline step exited non-zero. Check ECS task logs in CloudWatch."
      }
    }
  })

  logging_configuration {
    level                  = "ERROR"
    include_execution_data = false
    log_destination        = "${aws_cloudwatch_log_group.sfn.arn}:*"
  }
}

resource "aws_cloudwatch_log_group" "sfn" {
  name              = "/aws/states/${local.prefix}-ingestion"
  retention_in_days = 14
}

# ─── EventBridge Scheduler: daily at 09:00 UTC ───────────────────────────────

resource "aws_scheduler_schedule" "daily_ingestion" {
  name       = "${local.prefix}-daily-ingestion"
  group_name = "default"

  flexible_time_window {
    # Allow up to 15 min drift so AWS can bin-pack executions.
    # Acceptable for a non-latency-sensitive daily batch.
    mode                      = "FLEXIBLE"
    maximum_window_in_minutes = 15
  }

  # EventBridge cron syntax: cron(minute hour dom month dow year)
  schedule_expression          = var.ingestion_cron
  schedule_expression_timezone = "UTC"

  target {
    arn      = aws_sfn_state_machine.ingestion.arn
    role_arn = aws_iam_role.eventbridge_scheduler.arn

    retry_policy {
      maximum_retry_attempts       = 2
      maximum_event_age_in_seconds = 3600
    }
  }
}
