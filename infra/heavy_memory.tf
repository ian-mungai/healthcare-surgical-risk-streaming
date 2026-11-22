# The memory figure above which a batch job is heavy. A job is heavy when its group's summed memory limit is more than the memory that stays free beside one
# Spark job: the host's memory, less the reserve, the always-on limits and the Spark job's limit. The orchestrator never runs two heavy jobs together, and
# the deploy passes it this figure, so no figure is typed in code. The limits come from compose/limits.yml, the one source of every memory limit.
locals {
  limits = yamldecode(file("${path.module}/../compose/limits.yml"))

  # The group that every batch group depends on and that runs all the time, and the service of one Spark job.
  always_on_group = "supporting"
  spark_service   = "spark"

  always_on_memory_mib = sum([for service in values(local.limits.services) : service.memory_limit_mib if service.group == local.always_on_group])
  spark_memory_mib     = local.limits.services[local.spark_service].memory_limit_mib
  heavy_memory_mib     = data.aws_ec2_instance_type.host.memory_size - var.host_memory_reserve_mib - local.always_on_memory_mib - local.spark_memory_mib
}

data "aws_ec2_instance_type" "host" {
  instance_type = var.host_instance_type
}
