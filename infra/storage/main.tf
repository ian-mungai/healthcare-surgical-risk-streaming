# The provider reads the root sign-in from its own environment, which the storage command sets for each Terraform call from the host's files. No sign-in value is a
# variable, a provider argument or a value of this configuration, so none reaches the plan or the state.
provider "minio" {
  minio_server = var.endpoint
  minio_ssl    = true
}

# The one bucket of the project, on AIStor. It is private: no bucket policy is declared, and the project's S3 bucket has no lifecycle rule either.
resource "minio_s3_bucket" "data" {
  bucket        = var.bucket_name
  acl           = "private"
  force_destroy = false
}

# Versioning is on before the first write, as for the project's S3 bucket.
resource "minio_s3_bucket_versioning" "data" {
  bucket = minio_s3_bucket.data.bucket

  versioning_configuration {
    status = "Enabled"
  }
}
