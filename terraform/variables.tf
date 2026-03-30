variable "snowflake_account" {
	type        = string
	default     = null
	description = "Deprecated legacy account value. Prefer snowflake_organization_name + snowflake_account_name."
}

variable "snowflake_organization_name" {
	type        = string
	default     = null
	description = "Snowflake organization name (new provider field)."
}

variable "snowflake_account_name" {
	type        = string
	default     = null
	description = "Snowflake account name (new provider field)."
}

variable "snowflake_user" {
	type = string
}

variable "snowflake_password" {
	type      = string
	sensitive = true
}
