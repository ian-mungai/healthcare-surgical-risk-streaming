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

  github_repository   = "example-owner/example-repository"
  tailscale_client_id = "example-client-id"
  tailscale_audience  = "example-audience"
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

    workload_role_pattern = "example_project_workload_*"

    tailscale = {
      client_id = "example-client-id"
      audience  = "example-audience"
      tag       = "tag:example-host"
      host_name = "platform-dev"
      version   = "1.104.7"
    }
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
    error_message = "Instance metadata needs a session token and one network hop. Workloads on the host network have no hop, so the metadata guard role keeps them away."
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

run "host_joins_tailscale_without_a_stored_key" {
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

    workload_role_pattern = "example_project_workload_*"

    tailscale = {
      client_id = "example-client-id"
      audience  = "example-audience"
      tag       = "tag:example-host"
      host_name = "platform-dev"
      version   = "1.104.7"
    }
  }

  assert {
    condition = alltrue([
      jsondecode(aws_iam_role_policy.host_tailscale.policy).Statement[0].Action == "sts:GetWebIdentityToken",
      jsondecode(aws_iam_role_policy.host_tailscale.policy).Statement[0].Condition == {
        "ForAllValues:StringEquals" = { "sts:IdentityTokenAudience" = "example-audience" }
        Null                        = { "sts:IdentityTokenAudience" = "false" }
        NumericLessThanEquals       = { "sts:DurationSeconds" = "300" }
      },
    ])
    error_message = "The host may request an identity token only when every requested audience is the Tailscale audience, when an audience is given and for at most 300 seconds. ForAnyValue would admit a request that adds a second audience."
  }

  assert {
    condition = alltrue([
      strcontains(local.user_data, "--client-id='example-client-id?ephemeral=true&preauthorized=true'"),
      !strcontains(local.user_data, "ephemeral=false"),
      strcontains(local.user_data, "--audience='example-audience'"),
      strcontains(local.user_data, "--advertise-tags='tag:example-host'"),
      strcontains(local.user_data, "--hostname='platform-dev'"),
      strcontains(local.user_data, "tailscale=1.104.7"),
      !strcontains(local.user_data, "tskey"),
      !strcontains(local.user_data, "auth-key"),
    ])
    error_message = "The boot script registers the host as an ephemeral node with the federated identity, the tag and the neutral host name, and holds no auth key."
  }

  assert {
    condition = alltrue([
      strcontains(local.user_data, "/etc/systemd/system/tailscale-join.service"),
      strcontains(local.user_data, "Type=oneshot"),
      strcontains(local.user_data, "Restart=on-failure"),
      strcontains(local.user_data, "WantedBy=multi-user.target"),
      strcontains(local.user_data, "systemctl enable --now tailscale-join.service"),
      length(regexall("tailscale up", local.user_data)) == 1,
    ])
    error_message = "The join runs from a systemd unit that is enabled at boot and is the only place that runs tailscale up, so a stopped host rejoins at every start."
  }

  assert {
    condition     = strcontains(local.user_data, "After=network-online.target tailscaled.service") && !strcontains(local.user_data, "Before=")
    error_message = "The join unit orders itself after the network only, so it never slows a reboot or the services that start with it."
  }
}

run "deploy_role_trusts_only_the_protected_workflow" {
  command = plan

  module {
    source = "./modules/deploy"
  }

  variables {
    name               = "example_project_deploy_dev"
    boundary_name      = "example_project_role_boundary"
    github_repository  = "example-owner/example-repository"
    github_environment = "deploy"
  }

  assert {
    condition = jsondecode(aws_iam_role.deploy.assume_role_policy).Statement[0].Condition == {
      StringEquals = {
        "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com"
        "token.actions.githubusercontent.com:sub" = "repo:example-owner/example-repository:environment:deploy:ref:refs/heads/main:workflow_ref:example-owner/example-repository/.github/workflows/deploy.yml@refs/heads/main"
      }
    }
    error_message = "AWS maps only the audience and the subject of a GitHub token, so the trust policy matches the audience and one exact customized subject that carries the repository, the environment, the branch and the workflow file."
  }

  assert {
    condition     = jsondecode(aws_iam_role.deploy.assume_role_policy).Statement[0].Action == "sts:AssumeRoleWithWebIdentity" && endswith(jsondecode(aws_iam_role.deploy.assume_role_policy).Statement[0].Principal.Federated, ":oidc-provider/token.actions.githubusercontent.com")
    error_message = "Only GitHub's identity provider may assume the deploy role, and only with a web identity token."
  }

  assert {
    condition     = endswith(aws_iam_role.deploy.permissions_boundary, ":policy/example_project_role_boundary")
    error_message = "The deploy role carries the project's permissions boundary."
  }
}

run "given_host_is_not_provisioned" {
  command = plan

  variables {
    provision_host = false
  }

  assert {
    condition     = output.host_instance_id == null && output.host_role_arn == null
    error_message = "With a host that already exists, no instance and no instance role are planned."
  }

  assert {
    condition     = length(output.bucket_names) == 2
    error_message = "The project buckets are planned for a host that already exists too."
  }
}

run "each_workload_gets_its_own_role" {
  command = plan

  module {
    source = "./modules/workload_roles"
  }

  variables {
    name_prefix   = "example_project_workload"
    environment   = "dev"
    boundary_name = "example_project_role_boundary"
    host_role_arn = "arn:aws:iam::<ACCOUNT>:role/example_project_host_dev"
    workloads = {
      reader = { statements = [{ sid = "ReadLanding", actions = ["s3:GetObject"], resources = ["arn:aws:s3:::example-landing/*"] }] }
      writer = { statements = [{ sid = "WriteWarehouse", actions = ["s3:PutObject"], resources = ["arn:aws:s3:::example-warehouse/*"] }] }
    }
  }

  assert {
    condition     = { for name, role in aws_iam_role.workload : name => role.name } == { reader = "example_project_workload_reader_dev", writer = "example_project_workload_writer_dev" }
    error_message = "Each workload has its own role, named for the project, the workload and the environment."
  }

  assert {
    condition = alltrue([
      for role in aws_iam_role.workload :
      jsondecode(role.assume_role_policy).Statement[0].Principal == { AWS = "arn:aws:iam::<ACCOUNT>:role/example_project_host_dev" } && endswith(role.permissions_boundary, ":policy/example_project_role_boundary")
    ])
    error_message = "Only the host role may assume a workload role, and each workload role carries the permissions boundary."
  }

  assert {
    condition     = strcontains(aws_iam_role_policy.workload["reader"].policy, "example-landing") && !strcontains(aws_iam_role_policy.workload["reader"].policy, "example-warehouse")
    error_message = "A workload's policy holds its own statements only."
  }
}

run "host_may_assume_only_workload_roles" {
  command = plan

  module {
    source = "./modules/host"
  }

  variables {
    name                  = "example_project_host_dev"
    boundary_name         = "example_project_role_boundary"
    instance_type         = "m6a.2xlarge"
    disk_gib              = 200
    bucket_arns           = { landing = "arn:aws:s3:::example-landing", warehouse = "arn:aws:s3:::example-warehouse" }
    workload_role_pattern = "example_project_workload_*"

    tailscale = {
      client_id = "example-client-id"
      audience  = "example-audience"
      tag       = "tag:example-host"
      host_name = "platform-dev"
      version   = "1.104.7"
    }
  }

  assert {
    condition     = jsondecode(aws_iam_role_policy.host_workloads.policy).Statement[0].Action == "sts:AssumeRole" && endswith(jsondecode(aws_iam_role_policy.host_workloads.policy).Statement[0].Resource, ":role/example_project_workload_*")
    error_message = "The host role may assume the project's workload roles and no other role."
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
      toset(keys(aws_s3_bucket.this)) == toset(["landing", "warehouse"]),
      toset(keys(aws_s3_bucket_versioning.this)) == toset(keys(aws_s3_bucket.this)),
      toset(keys(aws_s3_bucket_public_access_block.this)) == toset(keys(aws_s3_bucket.this)),
    ])
    error_message = "Both controls exist for exactly the buckets, so the checks above cannot pass over an empty or partial control map."
  }

  assert {
    condition = alltrue([
      for block in aws_s3_bucket_public_access_block.this :
      block.block_public_acls && block.ignore_public_acls && block.block_public_policy && block.restrict_public_buckets
    ])
    error_message = "Each bucket has all four public access blocks on."
  }
}
