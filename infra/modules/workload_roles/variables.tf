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

variable "workloads" {
  description = "Workloads that get their own role, each with the statements of its permissions policy."
  type = map(object({
    statements = list(object({
      sid       = string
      actions   = list(string)
      resources = list(string)
    }))
  }))
}
