provider "aws" {
  region  = var.aws_region
  profile = var.aws_profile

  default_tags {
    tags = {
      Project     = var.project_name
      Environment = var.environment
    }
  }
}

locals {
  bucket_prefix = replace(var.project_name, "_", "-")
}

module "storage" {
  source = "./modules/storage"

  bucket_names = {
    landing   = "${local.bucket_prefix}-landing-${var.environment}-${var.run_id}"
    warehouse = "${local.bucket_prefix}-warehouse-${var.environment}-${var.run_id}"
  }
}

module "host" {
  source = "./modules/host"
  count  = var.provision_host ? 1 : 0

  name          = "${var.project_name}_host_${var.environment}"
  boundary_name = "${var.project_name}_role_boundary"
  instance_type = var.host_instance_type
  disk_gib      = var.host_disk_gib
  bucket_arns   = module.storage.bucket_arns

  tailscale = {
    client_id = var.tailscale_client_id
    audience  = var.tailscale_audience
    tag       = var.tailscale_host_tag
    host_name = "platform-${var.environment}"
    version   = var.tailscale_version
  }
}

module "deploy" {
  source = "./modules/deploy"

  name               = "${var.project_name}_deploy_${var.environment}"
  boundary_name      = "${var.project_name}_role_boundary"
  github_repository  = var.github_repository
  github_environment = var.github_environment
}
