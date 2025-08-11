
variable "create_objects" {
  type = list(object({
    key    = string
    source = string
    etag   = string
    tags   = map(string)
  }))
  description = "List of objects to create in the S3 bucket"
  default     = []
}

variable "bucket_id" {
  type        = string
  description = "ID of the S3 bucket where objects will be created"
}
