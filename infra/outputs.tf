output "host_instance_id" {
  description = "EC2 instance ID of the host."
  value       = module.host.instance_id
}

output "host_public_ip" {
  description = "Public address of the host, which admits no inbound connection."
  value       = module.host.public_ip
}

output "host_role_arn" {
  description = "ARN of the host's instance role."
  value       = module.host.role_arn
}

output "host_security_group_id" {
  description = "ID of the host's security group."
  value       = module.host.security_group_id
}

output "bucket_names" {
  description = "Names of the project's buckets, by component."
  value       = module.storage.bucket_names
}
