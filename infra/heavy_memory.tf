# The memory figure above which a batch job is heavy. A job is heavy when its group's summed memory limit is more than the memory that stays free beside one
# Spark job: the host's memory, less the reserve, the always-on limits and the Spark job's limit. The orchestrator never runs two heavy jobs together, and
# the deploy passes it this figure, so no figure is typed in code. The three limits arrive as variables: the Terraform wrapper stages them from the limits
# loader (scripts/stack/heavy_inputs.py), the one validated reader of compose/limits.yml, in heavy_inputs.tfvars.json.
# A plan that creates no host looks up no instance type, so a small type cannot fail it, and it has no figure.
locals {
  heavy_memory_mib = var.provision_host ? data.aws_ec2_instance_type.host[0].memory_size - var.host_reserve_mib - var.always_on_limits_mib - var.spark_limit_mib : null
}

data "aws_ec2_instance_type" "host" {
  count = var.provision_host ? 1 : 0

  instance_type = var.host_instance_type
}
