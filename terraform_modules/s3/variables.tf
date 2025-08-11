variable "bucket_name" {
  type        = string
  description = "The name of the S3 bucket"
}

variable "force_destroy" {
  type        = bool
  description = "A boolean that indicates all objects should be deleted from the bucket so that the bucket can be destroyed without error"
  default     = false
}