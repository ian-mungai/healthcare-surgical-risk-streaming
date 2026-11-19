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

output "bucket_names" {
  description = "Names of the project's buckets, by component."
  value       = module.storage.bucket_names
}

output "deploy_role_arn" {
  description = "ARN of the role the protected deploy workflow assumes."
  value       = module.deploy.role_arn
}
