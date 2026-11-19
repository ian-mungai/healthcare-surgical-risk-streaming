variable "aws_region" {
  description = "AWS region of every resource."
  type        = string

  validation {
    condition     = can(regex("^[a-z]{2}(-[a-z]+)+-[0-9]$", var.aws_region))
    error_message = "Give an AWS region name such as us-west-2."
  }
}

variable "aws_profile" {
  description = "Name of the project's AWS command-line profile."
  type        = string
}

variable "project_name" {
  description = "Project name with underscores, used in resource names and the Project tag."
  type        = string

  validation {
    condition     = can(regex("^[a-z][a-z0-9_]*$", var.project_name))
    error_message = "Use lower-case letters, digits and underscores."
  }
}

variable "environment" {
  description = "Environment name: dev or prod."
  type        = string

  validation {
    condition     = contains(["dev", "prod"], var.environment)
    error_message = "Use dev or prod."
  }
}

variable "run_id" {
  description = "ID of the run that owns these resources, from the run binding. It ends each bucket name."
  type        = string

  validation {
    condition     = can(regex("^[0-9]{8}t[0-9]{4}-[0-9a-f]{6}$", var.run_id))
    error_message = "Use the run ID that the state bootstrap step wrote to the run binding."
  }
}

variable "host_instance_type" {
  description = "EC2 instance type of the host."
  type        = string
  default     = "m6a.2xlarge"
}

variable "host_disk_gib" {
  description = "Size of the host's gp3 root disk in GiB. It can grow and cannot shrink."
  type        = number
  default     = 200

  validation {
    condition     = var.host_disk_gib >= 200
    error_message = "The host disk is at least 200 GiB."
  }
}
