mock_provider "minio" {}

variables {
  endpoint    = "storage-host.example.ts.net:9449"
  bucket_name = "example-project"
}

run "the_bucket_is_private_versioned_and_named_by_the_run" {
  command = plan

  assert {
    condition     = minio_s3_bucket.data.bucket == "example-project" && minio_s3_bucket.data.acl == "private"
    error_message = "The bucket takes the name the run gives and is private."
  }

  assert {
    condition     = minio_s3_bucket.data.force_destroy == false
    error_message = "The bucket is never emptied by a destroy."
  }

  assert {
    condition     = one(minio_s3_bucket_versioning.data.versioning_configuration).status == "Enabled" && output.versioning_status == "Enabled"
    error_message = "Versioning must be Enabled on the bucket."
  }
}

run "a_name_that_s3_would_refuse_is_refused_before_any_change" {
  command = plan

  variables {
    bucket_name = "Bad_Name"
  }

  expect_failures = [var.bucket_name]
}

run "an_endpoint_with_a_scheme_or_a_path_is_refused" {
  command = plan

  variables {
    endpoint = "https://storage-host.example.ts.net:9449/"
  }

  expect_failures = [var.endpoint]
}
