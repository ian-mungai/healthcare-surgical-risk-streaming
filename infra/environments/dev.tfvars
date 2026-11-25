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
  bronze = {
    access = {
      landing                 = ["read"]
      manifests               = ["read"]
      "warehouse/raw_records" = ["read"]
      "warehouse/bronze"      = ["read", "write", "delete"]
    }
  }
  silver = {
    access = {
      "warehouse/raw_records" = ["read"]
      "warehouse/bronze"      = ["read"]
      "warehouse/silver"      = ["read", "write", "delete"]
    }
  }
  gold = {
    access = {
      "warehouse/bronze" = ["read"]
      "warehouse/silver" = ["read"]
      "warehouse/gold"   = ["read", "write", "delete"]
    }
  }
  stream = {
    access = {
      "warehouse/feature" = ["read", "write", "delete"]
      "warehouse/stream"  = ["read", "write", "delete"]
    }
  }
  maintenance = {
    access = { warehouse = ["read", "write"] }
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
