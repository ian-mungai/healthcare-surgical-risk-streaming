variable "name" {
  description = "Name of the deploy role."
  type        = string
}

variable "boundary_name" {
  description = "Name of the permissions boundary policy that caps the deploy role."
  type        = string
}

variable "github_repository" {
  description = "GitHub repository that may deploy, as owner/name."
  type        = string
}

variable "github_owner_id" {
  description = "Numeric ID of the GitHub account that owns the repository. GitHub puts it in the token's subject."
  type        = string
}

variable "github_repository_id" {
  description = "Numeric ID of the GitHub repository. GitHub puts it in the token's subject."
  type        = string
}

variable "github_environment" {
  description = "Name of the protected GitHub deployment environment."
  type        = string
}
