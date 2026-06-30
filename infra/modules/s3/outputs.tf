output "bucket_name"         { value = aws_s3_bucket.lake.bucket }
output "bucket_arn"          { value = aws_s3_bucket.lake.arn }
output "athena_results_bucket" { value = aws_s3_bucket.athena_results.bucket }
