output "instance_id" {
  description = "EC2 instance ID of the host."
  value       = aws_instance.host.id
}

output "image_id" {
  description = "AMI of the host."
  value       = aws_instance.host.ami
}

output "role_arn" {
  description = "ARN of the host's instance role."
  value       = aws_iam_role.host.arn
}

output "security_group_id" {
  description = "ID of the host's security group."
  value       = aws_security_group.host.id
}
