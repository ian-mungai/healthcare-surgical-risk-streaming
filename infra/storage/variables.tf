variable "endpoint" {
  description = "The host and port of the AIStor S3 API as the owner's computer reaches it over Tailscale (the gateway's storage port), for example storage-host.example.ts.net:9449. The run gives it; it holds no credential."
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9.-]*:[0-9]{4,5}$", var.endpoint))
    error_message = "The endpoint must be a lower case host name and a port, with no scheme and no path."
  }
}

variable "bucket_name" {
  description = "The name of the project's one bucket, the S3_BUCKET value of the project configuration: all data lives in it under prefixes."
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$", var.bucket_name))
    error_message = "The bucket name must be 3 to 63 characters: lower case letters, digits and hyphens, starting and ending with a letter or a digit."
  }
}
