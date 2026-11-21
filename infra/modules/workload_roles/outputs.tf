output "role_arns" {
  description = "ARN of each workload's role, by workload name."
  value       = { for name, role in aws_iam_role.workload : name => role.arn }
}

output "policies" {
  description = "The permissions policy document of each workload's role, by workload name, for the plan's reviewer."
  value       = { for name, policy in aws_iam_role_policy.workload : name => policy.policy }
}
