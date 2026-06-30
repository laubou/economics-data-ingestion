variable "project_name" {
  description = "Prefix applied to all resource names."
  type        = string
  default     = "economics-pipeline"
}

variable "environment" {
  description = "Deployment environment: dev | int | uat | prod"
  type        = string
  validation {
    condition     = contains(["dev", "int", "uat", "prod"], var.environment)
    error_message = "environment must be one of: dev, int, uat, prod"
  }
}

variable "aws_region" {
  description = "AWS region for all resources."
  type        = string
  default     = "eu-west-1"
}

# ---- Networking ----

variable "vpc_cidr" {
  description = "CIDR block for the VPC."
  type        = string
  default     = "10.0.0.0/16"
}

# ---- MSK ----

variable "kafka_version" {
  description = "Apache Kafka version to run on MSK."
  type        = string
  default     = "3.5.1"
}

variable "msk_broker_instance_type" {
  description = "EC2 instance type for MSK brokers."
  type        = string
  default     = "kafka.m5.large"
}

variable "msk_num_brokers" {
  description = "Total number of MSK broker nodes (must be a multiple of AZ count)."
  type        = number
  default     = 3
}

variable "kafka_topic" {
  description = "Kafka topic name for the sales-events stream."
  type        = string
  default     = "sales-events"
}
