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

variable "manifest_writer" {
  description = "Name of the one workload that may write the manifests prefix."
  type        = string
  default     = "ingestion_check"
}

variable "records_writer" {
  description = "Name of the one workload that may write and delete the records_input prefix, which holds the record load's read copies."
  type        = string
  default     = "orchestrator"
}

variable "workloads" {
  description = "Workloads that get their own role. Each names the prefixes it reaches, with the modes read, write and delete for each, and whether it reads the publisher's dataset. The variable file gives no bucket and no resource pattern."
  type = map(object({
    access         = map(list(string))
    publisher_read = optional(bool, false)
  }))

  validation {
    condition     = alltrue([for workload in values(var.workloads) : alltrue([for prefix in keys(workload.access) : contains(["landing", "manifests", "warehouse", "records_input"], prefix)])])
    error_message = "A workload may name only the landing, manifests, warehouse and records_input prefixes. The state prefix, the airbyte prefix, another bucket and a wildcard are refused."
  }

  validation {
    condition     = alltrue([for workload in values(var.workloads) : alltrue([for modes in values(workload.access) : length(modes) > 0 && alltrue([for mode in modes : contains(["read", "write", "delete"], mode)])])])
    error_message = "Each prefix of a workload has at least one mode, and each mode is read, write or delete."
  }

  validation {
    condition     = alltrue([for workload in values(var.workloads) : alltrue([for prefix, modes in workload.access : prefix == "warehouse" || prefix == "records_input" || !contains(modes, "delete")])])
    error_message = "Only the warehouse and records_input prefixes may have the delete mode."
  }

  validation {
    condition     = alltrue([for name, workload in var.workloads : name == var.records_writer || length(setintersection(lookup(workload.access, "records_input", []), ["write", "delete"])) == 0])
    error_message = "Only the workload named in records_writer may write or delete the records_input prefix."
  }

  validation {
    condition     = alltrue([for name, workload in var.workloads : name == var.manifest_writer || !contains(lookup(workload.access, "manifests", []), "write")])
    error_message = "Only the workload named in manifest_writer may write the manifests prefix."
  }
}
