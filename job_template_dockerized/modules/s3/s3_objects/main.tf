
resource "aws_s3_object" "objects" {
  for_each = { for obj in var.create_objects : obj.key => obj }
  bucket   = var.bucket_id
  key      = each.value.key
  source   = each.value.source
  etag     = each.value.etag
  tags     = each.value.tags
}