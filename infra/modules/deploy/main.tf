data "aws_caller_identity" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
  issuer     = "token.actions.githubusercontent.com"
  branch     = "refs/heads/main"

  # AWS maps only the audience and the subject of a GitHub token. The owner sets this template as the repository's subject template with
  # `gh api --method PUT repos/<owner>/<repository>/actions/oidc/customization/sub --input infra/modules/deploy/oidc_subject_template.json`.
  # GitHub then builds the subject from these claims in this order, as key:value pairs joined by colons.
  subject_keys = jsondecode(file("${path.module}/oidc_subject_template.json")).include_claim_keys
  subject_values = {
    repo         = var.github_repository
    environment  = var.github_environment
    ref          = local.branch
    workflow_ref = "${var.github_repository}/.github/workflows/deploy.yml@${local.branch}"
  }
  subject = join(":", [for key in local.subject_keys : "${key}:${local.subject_values[key]}"])
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
          "${local.issuer}:aud" = "sts.amazonaws.com"
          "${local.issuer}:sub" = local.subject
        }
      }
    }]
  })
}
