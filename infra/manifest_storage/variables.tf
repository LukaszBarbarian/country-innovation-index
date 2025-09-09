variable "storage_account_name" {
  type = string
}

variable "container_name" {
  type = string
}

variable "manifests" {
  type = list(object({
    layer = string
    env   = string
    path  = string
  }))
}
