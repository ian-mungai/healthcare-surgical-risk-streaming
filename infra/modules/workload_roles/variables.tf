variable "name_prefix" {
  description = "Start of each workload role's name."
  type        = string
}

variable "environment" {
  description = "Environment name, which ends each workload role's name."
  type        = string
}

variable "boundary_name" {
  description = "Name of the permissions boundary policy that caps each workload role."
  type        = string
}

variable "host_role_arn" {
  description = "ARN of the host role, the only principal that may assume a workload role."
  type        = string
}

variable "bucket_name" {
  description = "Name of the run's bound bucket, from the run binding. Every resource pattern of a workload role is built from it."
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$", var.bucket_name))
    error_message = "Use the bucket name that the state bootstrap step wrote to the run binding."
  }
}

variable "data_prefixes" {
  description = "The prefixes of the bucket that a workload may name. The Terraform state lives under another prefix and is not in this list."
  type        = list(string)
}

variable "manifest_writer" {
  description = "Name of the one workload that may write the manifests prefix."
  type        = string
  default     = "ingestion_check"
}

variable "workloads" {
  description = "Workloads that get their own role. Each names the prefixes it reaches, with the modes read and write for each, and whether it reads the publisher's dataset. The variable file gives no bucket and no resource pattern."
  type = map(object({
    access         = map(list(string))
    publisher_read = optional(bool, false)
  }))

  validation {
    condition     = alltrue([for workload in values(var.workloads) : alltrue([for prefix in keys(workload.access) : contains(var.data_prefixes, prefix)])])
    error_message = "A workload may name only the prefixes in data_prefixes. The state prefix, another bucket and a wildcard are refused."
  }

  validation {
    condition     = alltrue([for workload in values(var.workloads) : alltrue([for modes in values(workload.access) : length(modes) > 0 && alltrue([for mode in modes : contains(["read", "write"], mode)])])])
    error_message = "Each prefix of a workload has at least one mode, and each mode is read or write."
  }

  validation {
    condition     = alltrue([for name, workload in var.workloads : name == var.manifest_writer || !contains(lookup(workload.access, "manifests", []), "write")])
    error_message = "Only the workload named in manifest_writer may write the manifests prefix."
  }
}
