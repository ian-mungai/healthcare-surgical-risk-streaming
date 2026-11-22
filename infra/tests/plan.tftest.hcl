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
}

run "environment_uses_the_one_bucket_it_is_given" {
  command = plan

  assert {
    condition     = output.data_bucket_name == "example-project" && output.data_prefixes == ["landing", "warehouse"]
    error_message = "The environment creates no bucket: it uses the given bucket and keeps its data under the landing and warehouse prefixes."
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
    bucket_arn    = "arn:aws:s3:::example-project"
    data_prefixes = ["landing", "warehouse"]
    read_prefixes = ["manifests"]

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
    condition = alltrue([
      aws_instance.host.tags == tomap({ Name = "example_project_host_dev" }),
      aws_instance.host.root_block_device[0].tags == null,
      aws_security_group.host.tags == tomap({ Name = "example_project_host_dev" }),
    ])
    error_message = "The host and its security group carry a Name tag, so a console shows which is which. The disk sets no tag of its own: a tag set there is written after creation, which the project profile may not do. The provider adds the Project and Environment tags."
  }

  assert {
    condition     = aws_instance.host.user_data_replace_on_change == true
    error_message = "The boot script runs only at an instance's first boot, so a changed boot script must plan a new host. An in-place update would store the new script and leave the old one in effect."
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

  assert {
    condition = alltrue([
      for statement in jsondecode(aws_iam_role_policy.host_storage.policy).Statement :
      statement.Resource == ["arn:aws:s3:::example-project/landing/*", "arn:aws:s3:::example-project/warehouse/*"] if statement.Sid == "ReadAndWriteProjectObjects"
    ])
    error_message = "The host reads and writes objects under the data prefixes only, never the Terraform state."
  }

  assert {
    condition = alltrue([
      for statement in jsondecode(aws_iam_role_policy.host_storage.policy).Statement :
      statement.Resource == "arn:aws:s3:::example-project" && statement.Condition == { StringLike = { "s3:prefix" = ["landing/*", "manifests/*", "warehouse/*"] } } if statement.Sid == "ListProjectDataPrefixes"
    ])
    error_message = "The host lists the project bucket under the data prefixes and the read-only prefix only."
  }

  assert {
    condition = alltrue([
      for statement in jsondecode(aws_iam_role_policy.host_storage.policy).Statement :
      statement.Action == ["s3:GetObject", "s3:GetObjectVersion"] && statement.Resource == ["arn:aws:s3:::example-project/manifests/*"] if statement.Sid == "ReadProjectReadOnlyObjects"
    ])
    error_message = "The host reads the manifests prefix and cannot write it."
  }

  assert {
    condition = alltrue([
      for statement in jsondecode(aws_iam_role_policy.host_storage.policy).Statement :
      !strcontains(jsonencode(statement.Resource), "manifests") if contains(["ReadAndWriteProjectObjects"], statement.Sid)
    ])
    error_message = "The host's write statement never names the manifests prefix."
  }

  assert {
    condition     = length([for statement in jsondecode(aws_iam_role_policy.host_storage.policy).Statement : statement.Sid if contains(["ReadAndWriteProjectObjects", "ListProjectDataPrefixes", "ReadProjectReadOnlyObjects"], statement.Sid)]) == 3
    error_message = "The three project statements exist, so the checks above cannot pass over a missing statement."
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
    bucket_arn    = "arn:aws:s3:::example-project"
    data_prefixes = ["landing", "warehouse"]
    read_prefixes = ["manifests"]

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
      strcontains(local.user_data, "--client-id='example-client-id?ephemeral=false&preauthorized=true'"),
      !strcontains(local.user_data, "ephemeral=true"),
      strcontains(local.user_data, "--audience='example-audience'"),
      strcontains(local.user_data, "--advertise-tags='tag:example-host'"),
      strcontains(local.user_data, "--hostname='platform-dev'"),
      strcontains(local.user_data, "tailscale=1.104.7"),
      !strcontains(local.user_data, "tskey"),
      !strcontains(local.user_data, "auth-key"),
    ])
    error_message = "The boot script registers the host with the federated identity, the tag and the neutral host name, and holds no auth key. The node is not ephemeral: Tailscale removes an ephemeral node while the host is stopped, and the host then stays outside the network at its next start."
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
    name                 = "example_project_deploy_dev"
    boundary_name        = "example_project_role_boundary"
    github_repository    = "example-owner/example-repository"
    github_owner_id      = "1234567"
    github_repository_id = "7654321"
    github_environment   = "deploy"
  }

  assert {
    condition = jsondecode(aws_iam_role.deploy.assume_role_policy).Statement[0].Condition == {
      StringEquals = {
        "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com"
        "token.actions.githubusercontent.com:sub" = "repo:example-owner@1234567/example-repository@7654321:environment:deploy:ref:refs/heads/main:workflow_ref:example-owner/example-repository/.github/workflows/deploy.yml@refs/heads/main"
      }
    }
    error_message = "AWS maps only the audience and the subject of a GitHub token, so the trust policy matches the audience and one exact customized subject that carries the repository with its immutable owner and repository IDs, the environment, the branch and the workflow file."
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
    condition     = output.data_bucket_name == "example-project"
    error_message = "A host that already exists uses the same bucket."
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
    bucket_name   = "example-project"
    workloads = {
      reader = { access = { landing = ["read"] } }
      writer = { access = { warehouse = ["write"] } }
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
    condition = alltrue([
      jsondecode(aws_iam_role_policy.workload["reader"].policy).Statement == [
        { Sid = "ListPrefixes", Effect = "Allow", Action = ["s3:ListBucket", "s3:ListBucketVersions"], Resource = "arn:aws:s3:::example-project", Condition = { StringLike = { "s3:prefix" = ["landing/*"] } } },
        { Sid = "ReadObjects", Effect = "Allow", Action = ["s3:GetObject", "s3:GetObjectVersion"], Resource = ["arn:aws:s3:::example-project/landing/*"] },
      ],
      jsondecode(aws_iam_role_policy.workload["writer"].policy).Statement == [
        { Sid = "WriteObjects", Effect = "Allow", Action = ["s3:AbortMultipartUpload", "s3:PutObject"], Resource = ["arn:aws:s3:::example-project/warehouse/*"] },
      ],
    ])
    error_message = "A workload's policy holds only what its prefixes and modes give: the reader lists and reads landing and the writer writes warehouse, in the bound bucket."
  }
}

run "project_workloads_reach_only_their_prefixes_in_the_bound_bucket" {
  command = plan

  module {
    source = "./modules/workload_roles"
  }

  variables {
    name_prefix   = "example_project_workload"
    environment   = "dev"
    boundary_name = "example_project_role_boundary"
    host_role_arn = "arn:aws:iam::<ACCOUNT>:role/example_project_host_dev"
    bucket_name   = "example-project-test-11212200abcdef"
    workloads = {
      airbyte = {
        access         = { landing = ["write"] }
        publisher_read = true
      }
      ingestion_check = { access = { landing = ["read"], manifests = ["read", "write"] } }
      manifest_reader = { access = { manifests = ["read"] } }
    }
  }

  assert {
    condition = jsondecode(aws_iam_role_policy.workload["airbyte"].policy).Statement == [
      { Sid = "WriteObjects", Effect = "Allow", Action = ["s3:AbortMultipartUpload", "s3:PutObject"], Resource = ["arn:aws:s3:::example-project-test-11212200abcdef/landing/*"] },
      { Sid = "ListPublisherDataset", Effect = "Allow", Action = "s3:ListBucket", Resource = "arn:aws:s3:::physionet-open", Condition = { StringLike = { "s3:prefix" = ["vitaldb/1.0.0/*"] } } },
      { Sid = "ReadPublisherDataset", Effect = "Allow", Action = "s3:GetObject", Resource = "arn:aws:s3:::physionet-open/vitaldb/1.0.0/*" },
    ]
    error_message = "The Airbyte workload writes under landing and reads the publisher's dataset, in the run's bound bucket, and nothing more: it keeps no logs or state in the bucket."
  }

  assert {
    condition = jsondecode(aws_iam_role_policy.workload["ingestion_check"].policy).Statement == [
      { Sid = "ListPrefixes", Effect = "Allow", Action = ["s3:ListBucket", "s3:ListBucketVersions"], Resource = "arn:aws:s3:::example-project-test-11212200abcdef", Condition = { StringLike = { "s3:prefix" = ["landing/*", "manifests/*"] } } },
      { Sid = "ReadObjects", Effect = "Allow", Action = ["s3:GetObject", "s3:GetObjectVersion"], Resource = ["arn:aws:s3:::example-project-test-11212200abcdef/landing/*", "arn:aws:s3:::example-project-test-11212200abcdef/manifests/*"] },
      { Sid = "WriteObjects", Effect = "Allow", Action = ["s3:AbortMultipartUpload", "s3:PutObject"], Resource = ["arn:aws:s3:::example-project-test-11212200abcdef/manifests/*"] },
    ]
    error_message = "The checksum workload reads landing, reads and writes manifests, and never touches the publisher's bucket."
  }

  assert {
    condition = alltrue([
      for policy in values(aws_iam_role_policy.workload) :
      alltrue([
        for statement in jsondecode(policy.policy).Statement :
        alltrue([
          for resource in flatten([statement.Resource]) :
          startswith(resource, "arn:aws:s3:::example-project-test-11212200abcdef") || startswith(resource, "arn:aws:s3:::physionet-open")
        ])
      ])
    ])
    error_message = "Every resource of every workload policy is in the run's bound bucket or the publisher's bucket."
  }

  assert {
    condition     = alltrue([for policy in values(aws_iam_role_policy.workload) : !strcontains(policy.policy, "/state/") && !strcontains(policy.policy, "\"*\"") && !strcontains(policy.policy, "s3:*")])
    error_message = "No workload policy names the state prefix, a wildcard resource or a wildcard action."
  }

  assert {
    condition = alltrue([
      for name, policy in aws_iam_role_policy.workload :
      name == "ingestion_check" || !contains(flatten([
        for statement in jsondecode(policy.policy).Statement : flatten([statement.Resource]) if statement.Sid == "WriteObjects"
      ]), "arn:aws:s3:::example-project-test-11212200abcdef/manifests/*")
    ])
    error_message = "Only the checksum workload may write the manifests prefix."
  }

  assert {
    condition = jsondecode(aws_iam_role_policy.workload["manifest_reader"].policy).Statement == [
      { Sid = "ListPrefixes", Effect = "Allow", Action = ["s3:ListBucket", "s3:ListBucketVersions"], Resource = "arn:aws:s3:::example-project-test-11212200abcdef", Condition = { StringLike = { "s3:prefix" = ["manifests/*"] } } },
      { Sid = "ReadObjects", Effect = "Allow", Action = ["s3:GetObject", "s3:GetObjectVersion"], Resource = ["arn:aws:s3:::example-project-test-11212200abcdef/manifests/*"] },
    ]
    error_message = "A reader workload may read the manifests prefix and nothing else."
  }
}

run "workload_naming_a_prefix_outside_the_allowed_list_is_refused" {
  command = plan

  module {
    source = "./modules/workload_roles"
  }

  variables {
    name_prefix   = "example_project_workload"
    environment   = "dev"
    boundary_name = "example_project_role_boundary"
    host_role_arn = "arn:aws:iam::<ACCOUNT>:role/example_project_host_dev"
    bucket_name   = "example-project"
    workloads     = { airbyte = { access = { landing = ["read"], state = ["read"] } } }
  }

  expect_failures = [var.workloads]
}

run "workload_naming_another_bucket_is_refused" {
  command = plan

  module {
    source = "./modules/workload_roles"
  }

  variables {
    name_prefix   = "example_project_workload"
    environment   = "dev"
    boundary_name = "example_project_role_boundary"
    host_role_arn = "arn:aws:iam::<ACCOUNT>:role/example_project_host_dev"
    bucket_name   = "example-project"
    workloads     = { airbyte = { access = { "../other-bucket/landing" = ["read"] } } }
  }

  expect_failures = [var.workloads]
}

run "warehouse_workloads_reach_only_the_warehouse_prefix" {
  command = plan

  module {
    source = "./modules/workload_roles"
  }

  variables {
    name_prefix   = "example_project_workload"
    environment   = "dev"
    boundary_name = "example_project_role_boundary"
    host_role_arn = "arn:aws:iam::<ACCOUNT>:role/example_project_host_dev"
    bucket_name   = "example-project"
    workloads = {
      spark = { access = { warehouse = ["read", "write", "delete"] } }
      trino = { access = { warehouse = ["read"] } }
    }
  }

  assert {
    condition = jsondecode(aws_iam_role_policy.workload["spark"].policy).Statement == [
      { Sid = "ListPrefixes", Effect = "Allow", Action = ["s3:ListBucket", "s3:ListBucketVersions"], Resource = "arn:aws:s3:::example-project", Condition = { StringLike = { "s3:prefix" = ["warehouse/*"] } } },
      { Sid = "ReadObjects", Effect = "Allow", Action = ["s3:GetObject", "s3:GetObjectVersion"], Resource = ["arn:aws:s3:::example-project/warehouse/*"] },
      { Sid = "WriteObjects", Effect = "Allow", Action = ["s3:AbortMultipartUpload", "s3:PutObject"], Resource = ["arn:aws:s3:::example-project/warehouse/*"] },
      { Sid = "DeleteObjects", Effect = "Allow", Action = ["s3:DeleteObject"], Resource = ["arn:aws:s3:::example-project/warehouse/*"] },
    ]
    error_message = "The Spark workload lists, reads, writes and deletes under warehouse in the bound bucket and does nothing else."
  }

  assert {
    condition = jsondecode(aws_iam_role_policy.workload["trino"].policy).Statement == [
      { Sid = "ListPrefixes", Effect = "Allow", Action = ["s3:ListBucket", "s3:ListBucketVersions"], Resource = "arn:aws:s3:::example-project", Condition = { StringLike = { "s3:prefix" = ["warehouse/*"] } } },
      { Sid = "ReadObjects", Effect = "Allow", Action = ["s3:GetObject", "s3:GetObjectVersion"], Resource = ["arn:aws:s3:::example-project/warehouse/*"] },
    ]
    error_message = "The Trino workload lists and reads under warehouse only."
  }

  assert {
    condition     = alltrue([for policy in values(aws_iam_role_policy.workload) : !strcontains(policy.policy, "landing") && !strcontains(policy.policy, "manifests") && !strcontains(policy.policy, "physionet")])
    error_message = "Neither warehouse workload lists or reaches landing, manifests or the publisher's bucket."
  }
}

run "replay_workload_lists_and_reads_only_landing_and_manifests" {
  command = plan

  module {
    source = "./modules/workload_roles"
  }

  variables {
    name_prefix   = "example_project_workload"
    environment   = "dev"
    boundary_name = "example_project_role_boundary"
    host_role_arn = "arn:aws:iam::<ACCOUNT>:role/example_project_host_dev"
    bucket_name   = "example-project"
    workloads = {
      replay = { access = { landing = ["read"], manifests = ["read"] } }
    }
  }

  assert {
    condition = jsondecode(aws_iam_role_policy.workload["replay"].policy).Statement == [
      { Sid = "ListPrefixes", Effect = "Allow", Action = ["s3:ListBucket", "s3:ListBucketVersions"], Resource = "arn:aws:s3:::example-project", Condition = { StringLike = { "s3:prefix" = ["landing/*", "manifests/*"] } } },
      { Sid = "ReadObjects", Effect = "Allow", Action = ["s3:GetObject", "s3:GetObjectVersion"], Resource = ["arn:aws:s3:::example-project/landing/*", "arn:aws:s3:::example-project/manifests/*"] },
    ]
    error_message = "The replay workload lists and reads landing and manifests in the bound bucket and does nothing else: no write, no delete, no warehouse and no publisher's bucket."
  }
}

run "delete_covers_only_the_prefix_that_names_it" {
  command = plan

  module {
    source = "./modules/workload_roles"
  }

  variables {
    name_prefix   = "example_project_workload"
    environment   = "dev"
    boundary_name = "example_project_role_boundary"
    host_role_arn = "arn:aws:iam::<ACCOUNT>:role/example_project_host_dev"
    bucket_name   = "example-project"
    workloads     = { spark = { access = { warehouse = ["delete"], landing = ["write"] } } }
  }

  assert {
    condition = jsondecode(aws_iam_role_policy.workload["spark"].policy).Statement == [
      { Sid = "WriteObjects", Effect = "Allow", Action = ["s3:AbortMultipartUpload", "s3:PutObject"], Resource = ["arn:aws:s3:::example-project/landing/*"] },
      { Sid = "DeleteObjects", Effect = "Allow", Action = ["s3:DeleteObject"], Resource = ["arn:aws:s3:::example-project/warehouse/*"] },
    ]
    error_message = "The delete statement names only the prefixes that give the delete mode, never the prefixes that give write."
  }
}

run "delete_is_refused_on_landing" {
  command = plan

  module {
    source = "./modules/workload_roles"
  }

  variables {
    name_prefix   = "example_project_workload"
    environment   = "dev"
    boundary_name = "example_project_role_boundary"
    host_role_arn = "arn:aws:iam::<ACCOUNT>:role/example_project_host_dev"
    bucket_name   = "example-project"
    workloads     = { spark = { access = { warehouse = ["read", "delete"], landing = ["delete"] } } }
  }

  expect_failures = [var.workloads]
}

run "delete_is_refused_on_manifests" {
  command = plan

  module {
    source = "./modules/workload_roles"
  }

  variables {
    name_prefix   = "example_project_workload"
    environment   = "dev"
    boundary_name = "example_project_role_boundary"
    host_role_arn = "arn:aws:iam::<ACCOUNT>:role/example_project_host_dev"
    bucket_name   = "example-project"
    workloads     = { ingestion_check = { access = { manifests = ["read", "write", "delete"] } } }
  }

  expect_failures = [var.workloads]
}

run "workload_naming_the_airbyte_prefix_is_refused" {
  command = plan

  module {
    source = "./modules/workload_roles"
  }

  variables {
    name_prefix   = "example_project_workload"
    environment   = "dev"
    boundary_name = "example_project_role_boundary"
    host_role_arn = "arn:aws:iam::<ACCOUNT>:role/example_project_host_dev"
    bucket_name   = "example-project"
    workloads     = { airbyte = { access = { airbyte = ["read", "write"] } } }
  }

  expect_failures = [var.workloads]
}

run "workload_naming_a_wildcard_prefix_is_refused" {
  command = plan

  module {
    source = "./modules/workload_roles"
  }

  variables {
    name_prefix   = "example_project_workload"
    environment   = "dev"
    boundary_name = "example_project_role_boundary"
    host_role_arn = "arn:aws:iam::<ACCOUNT>:role/example_project_host_dev"
    bucket_name   = "example-project"
    workloads     = { airbyte = { access = { "*" = ["read"] } } }
  }

  expect_failures = [var.workloads]
}

run "workload_cannot_supply_its_own_resource_patterns" {
  command = plan

  module {
    source = "./modules/workload_roles"
  }

  variables {
    name_prefix   = "example_project_workload"
    environment   = "dev"
    boundary_name = "example_project_role_boundary"
    host_role_arn = "arn:aws:iam::<ACCOUNT>:role/example_project_host_dev"
    bucket_name   = "example-project"
    workloads = {
      airbyte = {
        access     = { landing = ["read"] }
        statements = [{ sid = "ReadOther", actions = ["s3:GetObject"], resources = ["arn:aws:s3:::other-bucket/*"] }]
      }
    }
  }

  assert {
    condition     = !strcontains(aws_iam_role_policy.workload["airbyte"].policy, "other-bucket") && !strcontains(aws_iam_role_policy.workload["airbyte"].policy, "ReadOther")
    error_message = "A workload's own statements and resource patterns never reach its policy: only its prefixes and modes do."
  }

  assert {
    condition     = strcontains(aws_iam_role_policy.workload["airbyte"].policy, "arn:aws:s3:::example-project/landing/*")
    error_message = "The policy is built from the prefix and the bound bucket, so the check above cannot pass over an empty policy."
  }
}

run "workload_with_an_unknown_mode_is_refused" {
  command = plan

  module {
    source = "./modules/workload_roles"
  }

  variables {
    name_prefix   = "example_project_workload"
    environment   = "dev"
    boundary_name = "example_project_role_boundary"
    host_role_arn = "arn:aws:iam::<ACCOUNT>:role/example_project_host_dev"
    bucket_name   = "example-project"
    workloads     = { airbyte = { access = { landing = ["execute"] } } }
  }

  expect_failures = [var.workloads]
}

run "workload_with_no_mode_is_refused" {
  command = plan

  module {
    source = "./modules/workload_roles"
  }

  variables {
    name_prefix   = "example_project_workload"
    environment   = "dev"
    boundary_name = "example_project_role_boundary"
    host_role_arn = "arn:aws:iam::<ACCOUNT>:role/example_project_host_dev"
    bucket_name   = "example-project"
    workloads     = { airbyte = { access = { landing = [] } } }
  }

  expect_failures = [var.workloads]
}

run "only_the_checksum_workload_may_write_manifests" {
  command = plan

  module {
    source = "./modules/workload_roles"
  }

  variables {
    name_prefix   = "example_project_workload"
    environment   = "dev"
    boundary_name = "example_project_role_boundary"
    host_role_arn = "arn:aws:iam::<ACCOUNT>:role/example_project_host_dev"
    bucket_name   = "example-project"
    workloads     = { airbyte = { access = { manifests = ["read", "write"] } } }
  }

  expect_failures = [var.workloads]
}

run "malformed_workload_bucket_is_refused" {
  command = plan

  module {
    source = "./modules/workload_roles"
  }

  variables {
    name_prefix   = "example_project_workload"
    environment   = "dev"
    boundary_name = "example_project_role_boundary"
    host_role_arn = "arn:aws:iam::<ACCOUNT>:role/example_project_host_dev"
    bucket_name   = "other/bucket"
    workloads     = {}
  }

  expect_failures = [var.bucket_name]
}

run "environment_builds_workload_patterns_from_the_run_bucket" {
  command = plan

  variables {
    data_bucket_name = "example-project-test-11212200abcdef"
    workloads = {
      airbyte         = { access = { landing = ["write"] }, publisher_read = true }
      ingestion_check = { access = { landing = ["read"], manifests = ["read", "write"] } }
    }
  }

  assert {
    condition     = join(",", sort(keys(output.workload_policies))) == "airbyte,ingestion_check"
    error_message = "The Airbyte workload and the checksum workload each get a role."
  }

  assert {
    condition = alltrue([
      for policy in values(output.workload_policies) :
      alltrue([for statement in jsondecode(policy).Statement : strcontains(jsonencode(statement.Resource), "example-project-test-11212200abcdef") || strcontains(jsonencode(statement.Resource), "physionet-open")])
    ])
    error_message = "The environment builds each workload's resource patterns from the run's bound bucket, which the run supplies and no variable file names."
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
    bucket_arn            = "arn:aws:s3:::example-project"
    data_prefixes         = ["landing", "warehouse"]
    read_prefixes         = ["manifests"]
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

run "malformed_bucket_name_is_refused" {
  command = plan

  variables {
    data_bucket_name = "Example_Bucket"
  }

  expect_failures = [var.data_bucket_name]
}

run "heavy_figure_is_the_memory_free_beside_one_spark_job" {
  command = plan

  assert {
    condition     = output.heavy_memory_mib == 11008
    error_message = "With the 32768 MiB of the mocked host, the 4096 MiB reserve, the 13568 MiB of the supporting group in compose/limits.yml and the 4096 MiB of the spark service, the heavy figure is 11008 MiB. When a limit in the file changes, the figure changes with it: update this number in the same commit."
  }
}

run "heavy_figure_follows_the_host_memory" {
  command = plan

  override_data {
    target = data.aws_ec2_instance_type.host
    values = {
      memory_size = 65536
    }
  }

  assert {
    condition     = output.heavy_memory_mib == 43776
    error_message = "The figure is read from the instance type's memory: 65536 - 4096 - 13568 - 4096 is 43776 MiB."
  }
}

run "heavy_figure_follows_the_reserve" {
  command = plan

  variables {
    host_memory_reserve_mib = 5120
  }

  assert {
    condition     = output.heavy_memory_mib == 9984
    error_message = "A reserve of 5120 MiB leaves 9984 MiB: 32768 - 5120 - 13568 - 4096."
  }
}

run "reserve_default_is_the_reserve_of_the_limits_file" {
  command = plan

  assert {
    condition     = var.host_memory_reserve_mib == yamldecode(file("${path.module}/../compose/limits.yml")).host_reserve_mib
    error_message = "The reserve that the heavy figure uses must be the host_reserve_mib that the admission check reads from compose/limits.yml."
  }
}

run "heavy_figure_of_zero_is_refused" {
  command = plan

  variables {
    host_memory_reserve_mib = 15104
  }

  expect_failures = [output.heavy_memory_mib]
}

run "negative_heavy_figure_is_refused" {
  command = plan

  variables {
    host_memory_reserve_mib = 30000
  }

  expect_failures = [output.heavy_memory_mib]
}

run "zero_reserve_is_refused" {
  command = plan

  variables {
    host_memory_reserve_mib = 0
  }

  expect_failures = [var.host_memory_reserve_mib]
}

run "negative_reserve_is_refused" {
  command = plan

  variables {
    host_memory_reserve_mib = -1
  }

  expect_failures = [var.host_memory_reserve_mib]
}
