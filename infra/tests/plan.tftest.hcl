mock_provider "aws" {
  mock_data "aws_caller_identity" {
    defaults = {
      account_id = "111111111111"
    }
  }

  mock_data "aws_vpc" {
    defaults = {
      id = "vpc-0123456789abcdef0"
    }
  }

  mock_data "aws_subnets" {
    defaults = {
      ids = ["subnet-0bbbbbbbbbbbbbbbb", "subnet-0aaaaaaaaaaaaaaaa"]
    }
  }

  mock_data "aws_ssm_parameter" {
    defaults = {
      value = "ami-0123456789abcdef0"
    }
  }
}

variables {
  aws_region   = "us-west-2"
  aws_profile  = "example_project_dev"
  project_name = "example_project"
  environment  = "dev"
  run_id       = "20260101t0000-abcdef"
}

run "environment_names_follow_the_run" {
  command = plan

  assert {
    condition = output.bucket_names == {
      landing   = "example-project-landing-dev-20260101t0000-abcdef"
      warehouse = "example-project-warehouse-dev-20260101t0000-abcdef"
    }
    error_message = "Each bucket name is the project, the component, the environment and the run ID, with dashes."
  }
}

run "host_matches_the_design" {
  command = plan

  module {
    source = "./modules/host"
  }

  variables {
    name          = "example_project_host_dev"
    boundary_name = "example_project_role_boundary"
    instance_type = "m6a.2xlarge"
    disk_gib      = 200
    bucket_arns   = { landing = "arn:aws:s3:::example-landing", warehouse = "arn:aws:s3:::example-warehouse" }
  }

  assert {
    condition     = aws_instance.host.instance_type == "m6a.2xlarge" && aws_instance.host.ami == "ami-0123456789abcdef0"
    error_message = "The host is an m6a.2xlarge from the Ubuntu image parameter."
  }

  assert {
    condition = alltrue([
      aws_instance.host.root_block_device[0].volume_type == "gp3",
      aws_instance.host.root_block_device[0].volume_size == 200,
      aws_instance.host.root_block_device[0].iops == 3000,
      aws_instance.host.root_block_device[0].throughput == 125,
      aws_instance.host.root_block_device[0].encrypted,
    ])
    error_message = "The root disk is 200 GiB of encrypted gp3 with its included 3,000 IOPS and 125 MiB/s."
  }

  assert {
    condition     = aws_instance.host.metadata_options[0].http_tokens == "required" && aws_instance.host.metadata_options[0].http_put_response_hop_limit == 1
    error_message = "Instance metadata needs a session token and answers the host only."
  }

  assert {
    condition     = aws_instance.host.subnet_id == "subnet-0aaaaaaaaaaaaaaaa"
    error_message = "The host uses the first default subnet in sorted order, so a second plan chooses the same one."
  }

  assert {
    condition     = length(aws_security_group.host.ingress) == 0 && length(aws_security_group.host.egress) == 1
    error_message = "The security group has no inbound rule and one outbound rule."
  }

  assert {
    condition     = endswith(aws_iam_role.host.permissions_boundary, ":policy/example_project_role_boundary")
    error_message = "The host role carries the project's permissions boundary."
  }

  assert {
    condition     = !strcontains(aws_iam_role_policy.host_storage.policy, "\"Resource\":\"*\"") && !strcontains(aws_iam_role_policy.host_storage.policy, "s3:*")
    error_message = "The host's storage policy names its buckets and actions."
  }
}

run "small_disk_is_refused" {
  command = plan

  variables {
    host_disk_gib = 100
  }

  expect_failures = [var.host_disk_gib]
}

run "malformed_run_id_is_refused" {
  command = plan

  variables {
    run_id = "latest"
  }

  expect_failures = [var.run_id]
}

run "buckets_are_private_and_versioned" {
  command = plan

  module {
    source = "./modules/storage"
  }

  variables {
    bucket_names = { landing = "example-landing", warehouse = "example-warehouse" }
  }

  assert {
    condition     = length(aws_s3_bucket.this) == 2 && alltrue([for bucket in aws_s3_bucket_versioning.this : bucket.versioning_configuration[0].status == "Enabled"])
    error_message = "Each bucket has versioning enabled."
  }

  assert {
    condition = alltrue([
      for block in aws_s3_bucket_public_access_block.this :
      block.block_public_acls && block.ignore_public_acls && block.block_public_policy && block.restrict_public_buckets
    ])
    error_message = "Each bucket has all four public access blocks on."
  }
}
