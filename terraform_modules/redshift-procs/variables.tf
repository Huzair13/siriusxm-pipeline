variable "database"{ 
    type = string 
}
variable "workgroup_name" { 
    type = string
    default = null 
}
variable "cluster_identifier" { 
    type = string
    default = null 
}
variable "secret_arn" { 
    type = string 
}
variable "procedures" {
  type = map(object({ sql_path = string, schema = string }))
}
