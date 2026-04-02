variable "name_prefix" { type = string }
variable "common_tags" { type = map(string) }

variable "glue_role_arn" { type = string }
variable "config_bucket_id" { type = string }
variable "raw_bucket_id" { type = string }
variable "processed_bucket_id" { type = string }
variable "quarantine_bucket_id" { type = string }

variable "glue_script_path" {
  description = "Absolute path to the etl_job.py script."
  type        = string
}
