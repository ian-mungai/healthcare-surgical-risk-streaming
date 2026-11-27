output "bucket_name" {
  description = "The name of the project's bucket on AIStor."
  value       = minio_s3_bucket.data.bucket
}

output "versioning_status" {
  description = "The versioning status that the configuration sets on the bucket; the storage command asks AIStor for the live status too."
  value       = one(minio_s3_bucket_versioning.data.versioning_configuration).status
}
