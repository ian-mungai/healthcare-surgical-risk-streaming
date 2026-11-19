variable "name" {
  description = "Name of the host and of its role, instance profile and security group."
  type        = string
}

variable "boundary_name" {
  description = "Name of the permissions boundary policy that caps the host's role."
  type        = string
}

variable "instance_type" {
  description = "EC2 instance type."
  type        = string
}

variable "disk_gib" {
  description = "Size of the gp3 root disk in GiB."
  type        = number
}

variable "tailscale" {
  description = "Tailscale enrolment of the host: the federated identity's client ID and audience, the host's tag, its neutral host name and the client version."
  type = object({
    client_id = string
    audience  = string
    tag       = string
    host_name = string
    version   = string
  })
}

variable "workload_role_pattern" {
  description = "Name pattern of the workload roles the host may assume."
  type        = string
}

variable "bucket_arn" {
  description = "ARN of the project's one bucket."
  type        = string
}

variable "data_prefixes" {
  description = "Prefixes of the bucket the host may list, read and write. The Terraform state lives under another prefix."
  type        = list(string)
}
