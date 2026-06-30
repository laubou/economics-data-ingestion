variable "environment" {
  description = "Deployment environment (dev / int / uat / prod)"
  type        = string
}

variable "aws_region" {
  description = "AWS region"
  type        = string
}

variable "ecs_cluster_arn" {
  description = "ARN of the ECS cluster where pipeline tasks run"
  type        = string
}

variable "downloader_task_definition_arn" {
  description = "ARN of the ECS task definition for the downloader service"
  type        = string
}

variable "producer_task_definition_arn" {
  description = "ARN of the ECS task definition for the producer service"
  type        = string
}

variable "private_subnet_ids" {
  description = "Private subnet IDs where Fargate tasks run"
  type        = list(string)
}

variable "pipeline_security_group_id" {
  description = "Security group for pipeline ECS tasks"
  type        = string
}

variable "ingestion_cron" {
  description = "Cron expression for the daily batch trigger (UTC). Default: 09:00 UTC."
  type        = string
  default     = "cron(0 9 * * ? *)"
}
