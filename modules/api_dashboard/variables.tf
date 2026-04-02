variable "name_prefix" { type = string }
variable "common_tags" { type = map(string) }
variable "force_destroy" {
  type    = bool
  default = true
}

variable "api_query_invoke_arn" {
  description = "invoke_arn of the api_query Lambda."
  type        = string
}

variable "api_query_function_name" {
  description = "Function name of the api_query Lambda (for Lambda permission)."
  type        = string
}

variable "api_allowed_cors_origin" {
  type    = string
  default = "*"
}

variable "dashboard_html_path" {
  description = "Absolute path to dashboard/index.html."
  type        = string
}
