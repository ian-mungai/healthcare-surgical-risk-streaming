data "aws_caller_identity" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
  issuer     = "token.actions.githubusercontent.com"
}

# The role has no permissions policy yet: the deploy job proves its identity and reaches the host over Tailscale.
resource "aws_iam_role" "deploy" {
  name                 = var.name
  permissions_boundary = "arn:aws:iam::${local.account_id}:policy/${var.boundary_name}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "ProtectedDeployWorkflowOnMainOnly"
      Effect    = "Allow"
      Action    = "sts:AssumeRoleWithWebIdentity"
      Principal = { Federated = "arn:aws:iam::${local.account_id}:oidc-provider/${local.issuer}" }
      Condition = {
        StringEquals = {
          "${local.issuer}:aud"              = "sts.amazonaws.com"
          "${local.issuer}:sub"              = "repo:${var.github_repository}:environment:${var.github_environment}"
          "${local.issuer}:repository"       = var.github_repository
          "${local.issuer}:ref"              = "refs/heads/main"
          "${local.issuer}:environment"      = var.github_environment
          "${local.issuer}:job_workflow_ref" = "${var.github_repository}/.github/workflows/deploy.yml@refs/heads/main"
        }
      }
    }]
  })
}
