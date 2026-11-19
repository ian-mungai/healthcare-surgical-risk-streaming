output "role_arns" {
  description = "ARN of each workload's role, by workload name."
  value       = { for name, role in aws_iam_role.workload : name => role.arn }
}
