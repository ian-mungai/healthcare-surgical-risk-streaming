terraform {
  required_version = "~> 1.16.4"

  backend "local" {}

  required_providers {
    minio = {
      source  = "aminueza/minio"
      version = "= 3.46.2"
    }
  }
}
