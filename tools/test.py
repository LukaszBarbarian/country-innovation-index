from azure.identity import AzureCliCredential, DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

class AzureBlobHelper:
    def __init__(self, account_url, container_name):
        self.credential = DefaultAzureCredential()
        self.service_client = BlobServiceClient(account_url=account_url, credential=self.credential)
        self.container_client = self.service_client.get_container_client(container_name)

    def list_blobs(self):
        return self.container_client.list_blobs(name_starts_with="NOBELPRIZE/2025/08/01")








def main():
    helper = AzureBlobHelper("https://demosurdevdatalake4418sa.blob.core.windows.net", "bronze")
    print(helper.list_blobs())


if __name__ == "__main__":
    main()