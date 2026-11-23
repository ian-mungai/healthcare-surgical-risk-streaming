aws_region           = "us-west-2"
project_name         = "healthcare_surgical_risk_streaming"
environment          = "dev"
github_repository    = "ian-mungai/healthcare-surgical-risk-streaming"
github_owner_id      = "29179213"
github_repository_id = "1463269414"

workloads = {
  airbyte = {
    access         = { landing = ["write"] }
    publisher_read = true
  }
  ingestion_check = {
    access = { landing = ["read"], manifests = ["read", "write"] }
  }
  spark = {
    access = { warehouse = ["read", "write", "delete"] }
  }
  trino = {
    access = { warehouse = ["read"] }
  }
  replay = {
    access = { landing = ["read"], manifests = ["read"] }
  }
  orchestrator = {
    access = { landing = ["read"], manifests = ["read"], records_input = ["read", "write", "delete"] }
  }
}
