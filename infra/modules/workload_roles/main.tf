data "aws_caller_identity" "current" {}

resource "aws_iam_role" "workload" {
  for_each = var.workloads

  name                 = "${var.name_prefix}_${each.key}_${var.environment}"
  permissions_boundary = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:policy/${var.boundary_name}"
  max_session_duration = 3600

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "OnlyTheHostRoleAssumesThisWorkload"
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { AWS = var.host_role_arn }
    }]
  })
}

resource "aws_iam_role_policy" "workload" {
  for_each = var.workloads

  name = "workload"
  role = aws_iam_role.workload[each.key].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [for statement in each.value.statements : {
      Sid      = statement.sid
      Effect   = "Allow"
      Action   = statement.actions
      Resource = statement.resources
    }]
  })
}
