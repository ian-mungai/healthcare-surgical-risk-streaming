output "bucket_names" {
  description = "Bucket name for each component."
  value       = { for component, bucket in aws_s3_bucket.this : component => bucket.bucket }
}

output "bucket_arns" {
  description = "Bucket ARN for each component."
  value       = { for component, bucket in aws_s3_bucket.this : component => bucket.arn }
}
