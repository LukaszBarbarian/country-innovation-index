resource "azurerm_storage_blob" "manifest" {
  for_each = { for m in var.manifests : "${m.layer}-${m.env}" => m }

  name                   = "${each.value.env}.manifest.json"
  storage_account_name   = var.storage_account_name
  storage_container_name = each.value.container
  type                   = "Block"
  source                 = each.value.path
}
