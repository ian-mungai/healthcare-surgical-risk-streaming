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

variable "bucket_arns" {
  description = "ARN of each project bucket the host may read and write."
  type        = map(string)
}
