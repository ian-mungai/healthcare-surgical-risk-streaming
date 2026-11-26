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

variable "layer_writers" {
  description = "The warehouse prefixes that each hold the tables of one lake layer (and the catalog proof's own namespace), each with the name of the one workload that may write and delete it. Nessie's catalog puts a table in a folder of the warehouse prefix that is named for its namespace and the table, so the prefix of a layer is the folder of its namespace."
  type        = map(string)
  default = {
    "warehouse/bronze"        = "bronze"
    "warehouse/silver"        = "silver"
    "warehouse/gold"          = "gold"
    "warehouse/feature"       = "stream"
    "warehouse/stream"        = "stream"
    "warehouse/catalog_proof" = "maintenance"
  }
}

variable "compaction_workload" {
  description = "Name of the one workload that may also write every warehouse layer prefix, which the table maintenance needs to rewrite the files of every layer. It reads the whole warehouse prefix for its tag checks, never deletes, and never writes the whole warehouse prefix or raw_records."
  type        = string
  default     = "maintenance"
}

variable "workloads" {
  description = "Workloads that get their own role. Each names the prefixes it reaches, with the modes read, write and delete for each, and whether it reads the publisher's dataset. The variable file gives no bucket and no resource pattern."
  type = map(object({
    access         = map(list(string))
    publisher_read = optional(bool, false)
  }))

  validation {
    condition     = alltrue([for workload in values(var.workloads) : alltrue([for prefix in keys(workload.access) : contains(concat(["landing", "manifests", "warehouse", "warehouse/raw_records", "records_input"], keys(var.layer_writers)), prefix)])])
    error_message = "A workload may name only the landing, manifests, warehouse and records_input prefixes, the warehouse raw_records prefix and the warehouse layer prefixes. The state prefix, the airbyte prefix, another bucket and a wildcard are refused."
  }

  validation {
    condition     = alltrue([for workload in values(var.workloads) : alltrue([for modes in values(workload.access) : length(modes) > 0 && alltrue([for mode in modes : contains(["read", "write", "delete"], mode)])])])
    error_message = "Each prefix of a workload has at least one mode, and each mode is read, write or delete."
  }

  validation {
    condition     = alltrue([for workload in values(var.workloads) : alltrue([for prefix, modes in workload.access : prefix == "records_input" || contains(keys(var.layer_writers), prefix) || !contains(modes, "delete")])])
    error_message = "Only the records_input prefix and the warehouse layer prefixes may have the delete mode: no workload deletes in the whole warehouse prefix, in landing or in manifests."
  }

  validation {
    condition = alltrue([
      for name, workload in var.workloads : alltrue([
        for prefix, writer in var.layer_writers :
        writer == name || (name == var.compaction_workload && !contains(lookup(workload.access, prefix, []), "delete")) || length(setintersection(lookup(workload.access, prefix, []), ["write", "delete"])) == 0
      ])
    ])
    error_message = "Only the workload that layer_writers names for a warehouse layer prefix may write or delete it, and the compaction workload may also write it, never delete it."
  }

  validation {
    condition     = alltrue([for workload in values(var.workloads) : length(setintersection(lookup(workload.access, "warehouse/raw_records", []), ["write", "delete"])) == 0])
    error_message = "No workload may write or delete the warehouse raw_records prefix: the Airbyte load that owns it holds its own key."
  }

  validation {
    condition     = alltrue([for workload in values(var.workloads) : !contains(lookup(workload.access, "warehouse", []), "write")])
    error_message = "No workload may write the whole warehouse prefix: a role writes the layer prefixes it owns or compacts, so it can never write raw_records."
  }

  validation {
    condition     = alltrue([for name, workload in var.workloads : name != var.compaction_workload || !contains(flatten(values(workload.access)), "delete")])
    error_message = "The compaction workload deletes nothing: the owner decided that maintenance deletes nothing before teardown."
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
