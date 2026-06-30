output "s3_bucket_name" {
  description = "Name of the data lake S3 bucket."
  value       = module.s3.bucket_name
}

output "msk_bootstrap_brokers" {
  description = "MSK bootstrap broker string (TLS) — set as PIPELINE_KAFKA_BOOTSTRAP_SERVERS."
  value       = module.msk.bootstrap_brokers_tls
  sensitive   = true
}

output "glue_database_name" {
  description = "Glue catalog database — set as PIPELINE_GLUE_DATABASE."
  value       = module.glue.database_name
}

output "athena_workgroup" {
  description = "Athena workgroup name for querying bronze and silver tables."
  value       = module.athena.workgroup_name
}

output "pipeline_role_arn" {
  description = "IAM role ARN to assign to pipeline services (ECS tasks, Lambda, Glue jobs)."
  value       = module.iam.pipeline_role_arn
}
