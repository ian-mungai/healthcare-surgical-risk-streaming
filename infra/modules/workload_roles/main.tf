data "aws_caller_identity" "current" {}

locals {
  bucket_arn           = "arn:aws:s3:::${var.bucket_name}"
  publisher_bucket_arn = "arn:aws:s3:::physionet-open"
  publisher_prefix     = "vitaldb/1.0.0"

  read_prefixes   = { for name, workload in var.workloads : name => sort([for prefix, modes in workload.access : prefix if contains(modes, "read")]) }
  delete_prefixes = { for name, workload in var.workloads : name => sort([for prefix, modes in workload.access : prefix if contains(modes, "delete")]) }
  write_prefixes  = { for name, workload in var.workloads : name => sort([for prefix, modes in workload.access : prefix if contains(modes, "write")]) }

  # The read copies of the record load are put and deleted by exact version, so their prefix has its own statements with the fewest actions: no multipart
  # upload and no delete of a whole object. Every other prefix keeps the statements below.
  records_prefix = "records_input"

  statements = { for name, workload in var.workloads : name => [
    for statement in [
      {
        present = length(local.read_prefixes[name]) > 0
        value = {
          Sid       = "ListPrefixes"
          Effect    = "Allow"
          Action    = ["s3:ListBucket", "s3:ListBucketVersions"]
          Resource  = local.bucket_arn
          Condition = { StringLike = { "s3:prefix" = [for prefix in local.read_prefixes[name] : "${prefix}/*"] } }
        }
      },
      {
        present = length(local.read_prefixes[name]) > 0
        value = {
          Sid      = "ReadObjects"
          Effect   = "Allow"
          Action   = ["s3:GetObject", "s3:GetObjectVersion"]
          Resource = [for prefix in local.read_prefixes[name] : "${local.bucket_arn}/${prefix}/*"]
        }
      },
      {
        present = length([for prefix in local.write_prefixes[name] : prefix if prefix != local.records_prefix]) > 0
        value = {
          Sid      = "WriteObjects"
          Effect   = "Allow"
          Action   = ["s3:AbortMultipartUpload", "s3:PutObject"]
          Resource = [for prefix in local.write_prefixes[name] : "${local.bucket_arn}/${prefix}/*" if prefix != local.records_prefix]
        }
      },
      {
        present = contains(local.write_prefixes[name], local.records_prefix)
        value = {
          Sid      = "WriteRecordCopies"
          Effect   = "Allow"
          Action   = ["s3:PutObject"]
          Resource = ["${local.bucket_arn}/${local.records_prefix}/*"]
        }
      },
      {
        present = length([for prefix in local.delete_prefixes[name] : prefix if prefix != local.records_prefix]) > 0
        value = {
          Sid      = "DeleteObjects"
          Effect   = "Allow"
          Action   = ["s3:DeleteObject"]
          Resource = [for prefix in local.delete_prefixes[name] : "${local.bucket_arn}/${prefix}/*" if prefix != local.records_prefix]
        }
      },
      {
        present = contains(local.delete_prefixes[name], local.records_prefix)
        value = {
          Sid      = "DeleteRecordCopyVersions"
          Effect   = "Allow"
          Action   = ["s3:DeleteObjectVersion"]
          Resource = ["${local.bucket_arn}/${local.records_prefix}/*"]
        }
      },
      {
        present = workload.publisher_read
        value = {
          Sid       = "ListPublisherDataset"
          Effect    = "Allow"
          Action    = "s3:ListBucket"
          Resource  = local.publisher_bucket_arn
          Condition = { StringLike = { "s3:prefix" = ["${local.publisher_prefix}/*"] } }
        }
      },
      {
        present = workload.publisher_read
        value = {
          Sid      = "ReadPublisherDataset"
          Effect   = "Allow"
          Action   = "s3:GetObject"
          Resource = "${local.publisher_bucket_arn}/${local.publisher_prefix}/*"
        }
      },
    ] : statement.value if statement.present
  ] }
}

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
    Version   = "2012-10-17"
    Statement = local.statements[each.key]
  })
}
