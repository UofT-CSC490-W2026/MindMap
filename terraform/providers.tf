terraform {
  required_providers {
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "~> 0.80"
    }
  }
}

locals {
  # Legacy support: old var.snowflake_account may be either
  # 1) <org>-<account_name> or 2) account locator.
  legacy_account_parts = var.snowflake_account != null ? split("-", var.snowflake_account) : []

  resolved_organization_name = var.snowflake_organization_name != null ? var.snowflake_organization_name : (
    length(local.legacy_account_parts) > 1 ? local.legacy_account_parts[0] : null
  )

  resolved_account_name = var.snowflake_account_name != null ? var.snowflake_account_name : (
    length(local.legacy_account_parts) > 1
      ? join("-", slice(local.legacy_account_parts, 1, length(local.legacy_account_parts)))
      : var.snowflake_account
  )
}

provider "snowflake" {
  organization_name = local.resolved_organization_name
  account_name      = local.resolved_account_name
  user              = var.snowflake_user
  password          = var.snowflake_password
  role              = "SYSADMIN"
}
