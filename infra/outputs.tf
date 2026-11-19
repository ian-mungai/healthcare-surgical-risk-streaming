output "host_instance_id" {
  description = "EC2 instance ID of the host. Null for a host that already exists."
  value       = one(module.host[*].instance_id)
}

output "host_public_ip" {
  description = "Public address of the host, which admits no inbound connection."
  value       = one(module.host[*].public_ip)
}

output "host_role_arn" {
  description = "ARN of the host's instance role."
  value       = one(module.host[*].role_arn)
}

output "host_security_group_id" {
  description = "ID of the host's security group."
  value       = one(module.host[*].security_group_id)
}

output "data_bucket_name" {
  description = "Name of the project's one bucket, which the state bootstrap step creates."
  value       = var.data_bucket_name
}

output "data_prefixes" {
  description = "Prefixes of the bucket that hold the project's data."
  value       = local.data_prefixes
}

output "deploy_role_arn" {
  description = "ARN of the role the protected deploy workflow assumes."
  value       = module.deploy.role_arn
}

output "workload_role_arns" {
  description = "ARN of each workload's role, by workload name. Empty for a host that already exists."
  value       = try(module.workload_roles[0].role_arns, {})
}
