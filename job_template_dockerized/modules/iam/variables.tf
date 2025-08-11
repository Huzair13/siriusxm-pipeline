variable "role_name" {
  description = "Name of the IAM role"
  type        = string
}

variable "policy_name" {
  description = "Name of the IAM policy"
  type        = string
}

variable "service_principal" {
  description = "AWS service principal that can assume this role"
  type        = string
}

variable "policy_statements" {
  description = "List of policy statements for the IAM role"
  type        = list(object({
    Effect    = string
    Action    = list(string)
    Resource  = any
  }))
}
