# The host has no Elastic IP, so AWS gives its instance a new public address and a new public DNS name on every `make host-start`. The plan review gate counts each
# output change on purpose, so a root output that holds one of those values turns every `make host-start` into a plan change that stops for an owner review.
# This run gives the instance a marker address and an EC2-shaped marker DNS name (ec2-<address with dashes>.<region>.compute.amazonaws.com) and plans. The mock data
# sources that can return the instance's address or DNS name return the same markers. It asserts nothing itself: tests/slow/test_host_outputs.py runs this file with
# `terraform test -json -verbose`, reads the exit code and the planned value of every root output from the plan and fails when Terraform exits with an error,
# when an output holds a marker or when the plan does not show both markers on the instance. A new output needs no entry here. An output that the plan cannot know needs a fixed value in an override_resource block below.

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

  # A lookup of the host returns the same markers as the instance itself. Otherwise a mock gives it an unrelated string, and an output that reads
  # the lookup instead of the resource passes.
  mock_data "aws_instance" {
    defaults = {
      public_ip  = "198.51.100.77"
      public_dns = "ec2-198-51-100-77.us-west-2.compute.amazonaws.com"
    }
  }

  mock_data "aws_instances" {
    defaults = {
      public_ips = ["198.51.100.77"]
    }
  }

  mock_data "aws_eip" {
    defaults = {
      public_ip  = "198.51.100.77"
      public_dns = "ec2-198-51-100-77.us-west-2.compute.amazonaws.com"
    }
  }

  mock_data "aws_eips" {
    defaults = {
      public_ips = ["198.51.100.77"]
    }
  }

  mock_data "aws_network_interface" {
    defaults = {
      association = [{
        public_ip       = "198.51.100.77"
        public_dns_name = "ec2-198-51-100-77.us-west-2.compute.amazonaws.com"
      }]
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
      public_dns = "ec2-198-51-100-77.us-west-2.compute.amazonaws.com"
    }
  }

  # A plan leaves the computed IDs and ARNs below unknown, and an unknown output cannot be checked. Each gets a fixed value that holds no marker.
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
}
