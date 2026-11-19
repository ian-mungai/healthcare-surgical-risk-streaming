data "aws_caller_identity" "current" {}

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }

  filter {
    name   = "default-for-az"
    values = ["true"]
  }
}

data "aws_ssm_parameter" "ubuntu" {
  name = "/aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id"
}

locals {
  publisher_bucket_arn = "arn:aws:s3:::physionet-open"
  user_data            = templatefile("${path.module}/tailscale_join.sh.tftpl", var.tailscale)
}

resource "aws_iam_role" "host" {
  name                 = var.name
  permissions_boundary = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:policy/${var.boundary_name}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "HostInstanceAssumesItsRole"
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { Service = "ec2.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "host_storage" {
  name = "storage"
  role = aws_iam_role.host.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ListProjectAndPublisherBuckets"
        Effect   = "Allow"
        Action   = ["s3:GetBucketLocation", "s3:ListBucket", "s3:ListBucketVersions"]
        Resource = concat(sort(values(var.bucket_arns)), [local.publisher_bucket_arn])
      },
      {
        Sid      = "ReadAndWriteProjectObjects"
        Effect   = "Allow"
        Action   = ["s3:AbortMultipartUpload", "s3:DeleteObject", "s3:GetObject", "s3:GetObjectVersion", "s3:PutObject"]
        Resource = [for arn in sort(values(var.bucket_arns)) : "${arn}/*"]
      },
      {
        Sid      = "ReadPublisherObjects"
        Effect   = "Allow"
        Action   = "s3:GetObject"
        Resource = "${local.publisher_bucket_arn}/vitaldb/1.0.0/*"
      },
    ]
  })
}

resource "aws_iam_role_policy" "host_tailscale" {
  name = "tailscale_identity"
  role = aws_iam_role.host.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid      = "RequestShortIdentityTokenForTailscale"
      Effect   = "Allow"
      Action   = "sts:GetWebIdentityToken"
      Resource = "*"
      Condition = {
        "ForAnyValue:StringEquals" = { "sts:IdentityTokenAudience" = var.tailscale.audience }
        NumericLessThanEquals      = { "sts:DurationSeconds" = "300" }
      }
    }]
  })
}

resource "aws_iam_instance_profile" "host" {
  name = var.name
  role = aws_iam_role.host.name
}

resource "aws_security_group" "host" {
  name        = var.name
  description = "Host with no inbound rule. All access comes through Tailscale."
  vpc_id      = data.aws_vpc.default.id
  ingress     = []

  egress {
    description = "Outbound to any address"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_instance" "host" {
  ami                         = data.aws_ssm_parameter.ubuntu.value
  instance_type               = var.instance_type
  subnet_id                   = sort(data.aws_subnets.default.ids)[0]
  vpc_security_group_ids      = [aws_security_group.host.id]
  iam_instance_profile        = aws_iam_instance_profile.host.name
  associate_public_ip_address = true
  user_data                   = local.user_data

  metadata_options {
    http_tokens                 = "required"
    http_put_response_hop_limit = 1
  }

  root_block_device {
    volume_type           = "gp3"
    volume_size           = var.disk_gib
    iops                  = 3000
    throughput            = 125
    encrypted             = true
    delete_on_termination = true
  }

  lifecycle {
    ignore_changes = [ami]
  }
}
