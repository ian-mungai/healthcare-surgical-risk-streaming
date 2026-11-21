ui = true
disable_mlock = true
api_addr = "http://127.0.0.1:8200"
cluster_addr = "http://127.0.0.1:8201"

storage "raft" {
  path    = "/openbao/file"
  node_id = "openbao_1"
}

listener "tcp" {
  address         = "0.0.0.0:8200"
  cluster_address = "127.0.0.1:8201"
  tls_disable     = true
}

seal "static" {
  current_key_id = "host_key_1"
  current_key    = "env://OPENBAO_UNLOCK_KEY"
}

initialize "first_start" {
  request "mount_secrets" {
    operation = "update"
    path      = "sys/mounts/secret"
    data = {
      type = "kv"
      options = {
        version = "2"
      }
    }
  }

  request "enable_approle" {
    operation = "update"
    path      = "sys/auth/approle"
    data = {
      type = "approle"
    }
  }

  request "write_admin_policy" {
    operation = "update"
    path      = "sys/policies/acl/bootstrap_admin"
    data = {
      policy = <<-EOT
        path "sys/policies/acl" { capabilities = ["list"] }
        path "sys/policies/acl/service_*" { capabilities = ["create", "read", "update", "list"] }
        path "auth/approle/role" { capabilities = ["list"] }
        path "auth/approle/role/*" { capabilities = ["create", "read", "update", "list"] }
        path "secret/data/services/*" { capabilities = ["create", "read", "update"] }
        path "secret/metadata/services/*" { capabilities = ["read", "list"] }
      EOT
    }
  }

  request "create_admin_role" {
    operation = "update"
    path      = "auth/approle/role/bootstrap"
    data = {
      role_id            = "bootstrap"
      token_policies     = ["bootstrap_admin"]
      token_ttl          = "10m"
      token_max_ttl      = "10m"
      secret_id_ttl      = 0
      secret_id_num_uses = 0
    }
  }

  request "set_admin_secret_id" {
    operation = "update"
    path      = "auth/approle/role/bootstrap/custom-secret-id"
    data = {
      secret_id = {
        eval_source = "env"
        eval_type   = "string"
        env_var     = "OPENBAO_BOOTSTRAP_SECRET_ID"
        require_present = true
      }
    }
  }
}
