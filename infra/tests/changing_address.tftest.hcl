# The host has no Elastic IP, so AWS gives its instance a new public address and a new public DNS name on every start. The plan review gate counts each
# output change on purpose, so a root output that holds one of those values turns every `make host-start` into a plan change that stops for an owner review.
# This run gives the instance a marker address and a marker DNS name, plans, and fails when any root output holds either marker. It tests the evaluated
# value of each output, so a local, an alias, a splat, a `for` projection or a whole-instance passthrough cannot hide it. A module output reaches the
# outside only through a root output, so the root outputs cover the module outputs. Every root output needs one assertion here: the fast test
# tests/fast/test_host_outputs.py fails for a root output that this file does not name.

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

  mock_data "aws_ec2_instance_type" {
    defaults = {
      memory_size = 32768
    }
  }
}

variables {
  aws_region   = "us-west-2"
  aws_profile  = "example_project_dev"
  project_name = "example_project"
  environment  = "dev"

  data_bucket_name = "example-project"

  github_repository    = "example-owner/example-repository"
  github_owner_id      = "1234567"
  github_repository_id = "7654321"
  tailscale_client_id  = "example-client-id"
  tailscale_audience   = "example-audience"

  host_reserve_mib     = 4096
  always_on_limits_mib = 13568
  spark_limit_mib      = 4096
}

run "no_root_output_holds_an_address_that_changes_on_every_start" {
  command = plan

  override_resource {
    target          = module.host[0].aws_instance.host
    override_during = plan
    values = {
      public_ip  = "198.51.100.77"
      public_dns = "marker-ip.invalid"
    }
  }

  # A plan leaves the computed IDs and ARNs below unknown, and an assertion cannot read an unknown value. Each gets a fixed value that holds no marker.
  override_resource {
    target          = module.host[0].aws_iam_role.host
    override_during = plan
    values = {
      arn = "arn:aws:iam::111111111111:role/example_project_host_dev"
    }
  }

  override_resource {
    target          = module.host[0].aws_security_group.host
    override_during = plan
    values = {
      id = "sg-0123456789abcdef0"
    }
  }

  override_resource {
    target          = module.deploy.aws_iam_role.deploy
    override_during = plan
    values = {
      arn = "arn:aws:iam::111111111111:role/example_project_deploy_dev"
    }
  }

  assert {
    condition     = alltrue([for marker in ["198.51.100.77", "marker-ip.invalid"] : !strcontains(nonsensitive(jsonencode(output.host_instance_id)), marker)])
    error_message = "The root output host_instance_id holds the host's public address or public DNS name, which change on every start."
  }

  assert {
    condition     = alltrue([for marker in ["198.51.100.77", "marker-ip.invalid"] : !strcontains(nonsensitive(jsonencode(output.host_image_id)), marker)])
    error_message = "The root output host_image_id holds the host's public address or public DNS name, which change on every start."
  }

  assert {
    condition     = alltrue([for marker in ["198.51.100.77", "marker-ip.invalid"] : !strcontains(nonsensitive(jsonencode(output.host_role_arn)), marker)])
    error_message = "The root output host_role_arn holds the host's public address or public DNS name, which change on every start."
  }

  assert {
    condition     = alltrue([for marker in ["198.51.100.77", "marker-ip.invalid"] : !strcontains(nonsensitive(jsonencode(output.host_security_group_id)), marker)])
    error_message = "The root output host_security_group_id holds the host's public address or public DNS name, which change on every start."
  }

  assert {
    condition     = alltrue([for marker in ["198.51.100.77", "marker-ip.invalid"] : !strcontains(nonsensitive(jsonencode(output.data_bucket_name)), marker)])
    error_message = "The root output data_bucket_name holds the host's public address or public DNS name, which change on every start."
  }

  assert {
    condition     = alltrue([for marker in ["198.51.100.77", "marker-ip.invalid"] : !strcontains(nonsensitive(jsonencode(output.data_prefixes)), marker)])
    error_message = "The root output data_prefixes holds the host's public address or public DNS name, which change on every start."
  }

  assert {
    condition     = alltrue([for marker in ["198.51.100.77", "marker-ip.invalid"] : !strcontains(nonsensitive(jsonencode(output.deploy_role_arn)), marker)])
    error_message = "The root output deploy_role_arn holds the host's public address or public DNS name, which change on every start."
  }

  assert {
    condition     = alltrue([for marker in ["198.51.100.77", "marker-ip.invalid"] : !strcontains(nonsensitive(jsonencode(output.workload_role_arns)), marker)])
    error_message = "The root output workload_role_arns holds the host's public address or public DNS name, which change on every start."
  }

  assert {
    condition     = alltrue([for marker in ["198.51.100.77", "marker-ip.invalid"] : !strcontains(nonsensitive(jsonencode(output.workload_policies)), marker)])
    error_message = "The root output workload_policies holds the host's public address or public DNS name, which change on every start."
  }

  assert {
    condition     = alltrue([for marker in ["198.51.100.77", "marker-ip.invalid"] : !strcontains(nonsensitive(jsonencode(output.heavy_memory_mib)), marker)])
    error_message = "The root output heavy_memory_mib holds the host's public address or public DNS name, which change on every start."
  }
}
