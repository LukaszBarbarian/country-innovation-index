resource "azurerm_storage_blob" "manifest" {
  for_each = { for m in var.manifests : "${m.layer}-${m.env}" => m }

  name                   = "${each.value.layer}/${each.value.env}.manifest.json"
  storage_account_name   = var.storage_account_name
  storage_container_name = var.container_name
  type                   = "Block"
  source                 = each.value.path
}
