variable "name_prefix" {
  type = string
}

variable "common_tags" {
  type = map(string)
}

variable "alert_email_subscriptions" {
  type    = list(string)
  default = []
}

variable "existing_redshift_secret_name" {
  description = "Optional existing Secrets Manager secret name for Redshift credentials."
  type        = string
  default     = ""
}

variable "redshift_admin_username" {
  description = "Used only when creating a new Redshift credentials secret."
  type        = string
}

variable "redshift_admin_password" {
  description = "Used only when creating a new Redshift credentials secret."
  type        = string
  sensitive   = true
}

variable "redshift_database" {
  description = "Used only when creating a new Redshift credentials secret."
  type        = string
}
