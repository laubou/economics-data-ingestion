terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Remote state — fill in backend-config at init time:
  # terraform init -backend-config=environments/<env>.backend.hcl
  backend "s3" {}
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}

# ------------------------------------------------------------------ #
# Networking (VPC, subnets, security groups)                           #
# ------------------------------------------------------------------ #
module "networking" {
  source       = "./modules/networking"
  project_name = var.project_name
  environment  = var.environment
  vpc_cidr     = var.vpc_cidr
  aws_region   = var.aws_region
}

# ------------------------------------------------------------------ #
# S3 — data lake bucket                                                #
# ------------------------------------------------------------------ #
module "s3" {
  source       = "./modules/s3"
  project_name = var.project_name
  environment  = var.environment
  aws_region   = var.aws_region
}

# ------------------------------------------------------------------ #
# MSK (Amazon Managed Streaming for Apache Kafka)                      #
# ------------------------------------------------------------------ #
module "msk" {
  source               = "./modules/msk"
  project_name         = var.project_name
  environment          = var.environment
  vpc_id               = module.networking.vpc_id
  subnet_ids           = module.networking.private_subnet_ids
  security_group_id    = module.networking.msk_security_group_id
  kafka_version        = var.kafka_version
  broker_instance_type = var.msk_broker_instance_type
  num_brokers          = var.msk_num_brokers
  kafka_topic          = var.kafka_topic
}

# ------------------------------------------------------------------ #
# Glue Data Catalog                                                    #
# ------------------------------------------------------------------ #
module "glue" {
  source        = "./modules/glue"
  project_name  = var.project_name
  environment   = var.environment
  s3_bucket     = module.s3.bucket_name
  s3_bucket_arn = module.s3.bucket_arn
}

# ------------------------------------------------------------------ #
# Athena workgroup + query results bucket                              #
# ------------------------------------------------------------------ #
module "athena" {
  source        = "./modules/athena"
  project_name  = var.project_name
  environment   = var.environment
  s3_bucket     = module.s3.bucket_name
  s3_bucket_arn = module.s3.bucket_arn
  glue_database = module.glue.database_name
}

# ------------------------------------------------------------------ #
# IAM roles for pipeline services                                      #
# ------------------------------------------------------------------ #
module "iam" {
  source        = "./modules/iam"
  project_name  = var.project_name
  environment   = var.environment
  s3_bucket_arn = module.s3.bucket_arn
  msk_arn       = module.msk.cluster_arn
  glue_database = module.glue.database_name
  aws_region    = var.aws_region
  account_id    = data.aws_caller_identity.current.account_id
}

data "aws_caller_identity" "current" {}

# ------------------------------------------------------------------ #
# Scheduler — daily batch trigger (EventBridge → Step Functions → ECS) #
# ------------------------------------------------------------------ #
module "scheduler" {
  source = "./modules/scheduler"

  environment  = var.environment
  aws_region   = var.aws_region
  ingestion_cron = "cron(0 9 * * ? *)"  # daily at 09:00 UTC

  ecs_cluster_arn                = module.ecs.cluster_arn
  downloader_task_definition_arn = module.ecs.downloader_task_definition_arn
  producer_task_definition_arn   = module.ecs.producer_task_definition_arn
  private_subnet_ids             = module.networking.private_subnet_ids
  pipeline_security_group_id     = module.networking.msk_security_group_id
}
