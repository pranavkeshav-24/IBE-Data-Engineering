variable "name_prefix" {
  description = "Name prefix for all S3 resources."
  type        = string
}

variable "common_tags" {
  description = "Tags applied to all resources."
  type        = map(string)
}

variable "force_destroy" {
  description = "Allow terraform destroy on non-empty buckets."
  type        = bool
  default     = true
}

variable "client_configs" {
  description = "Map of S3 key => JSON configuration content."
  type        = map(string)
}
