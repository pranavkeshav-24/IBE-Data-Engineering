variable "name_prefix" { type = string }
variable "common_tags" { type = map(string) }

variable "redshift_admin_username" { type = string }
variable "redshift_admin_password" {
  type      = string
  sensitive = true
}
variable "redshift_database" { type = string }
variable "redshift_s3_role_arn" { type = string }

variable "subnet_ids" {
  description = "Default VPC subnet IDs for the Redshift workgroup."
  type        = list(string)
}
