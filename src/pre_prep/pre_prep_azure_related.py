# pytest all possible test cases with monkeypatch for the below code

from azure.identity import ManagedIdentityCredential
from azure.storage.filedatalake import DataLakeFileClient

from pyspark.sql import SparkSession

from azure.core.exceptions import (
    ClientAuthenticationError # if tenant_id, client_id or client_secret is incorrect
)


class PreProcessingUtils:
    def __init__(self, lc, source_system_type, file_type_to_process, dbutils=None):
        self.lc = lc
        self.file_type_to_process = file_type_to_process
    
    def _get_az_credential(self) -> ManagedIdentityCredential:
        this_module = f"[{type("
        try:
            az_credential = ManagedIdentityCredential()
            az_credential.get_token(
                "https://storage.azure.com/.default"
            )
            return az_credential
        except ClientAuthenticationError as e:
            print(f"[{this_module_name}.{type(self}.__name__}.{this_method_name}] - Cannot obtain the Azure access token to access ADLS ({e})")
            raise
        except Exception as e:
            print(f"[{this_module_name}.{type(self}.__name__}.{this_method_name}] - Error setting up ManagedIdentityCredential ({e})")
            raise
    
    def get_file_data(self, passed_account_url, passed_container_name, passed_file_path):
        if type(passed_account_url) is not str:
            raise TypeError("passed_account_url must be string")
        
        if type(passed_container_name) is not str:
            raise TypeError("passed_container_name must be string")
        
        if type(passed_file_path) is not str:
            raise TypeError("passed_file_path must be string")
        
        az_credential = self._get_az_credential()
        az_client = DataLakeFileClient(
            account_url=passed_account_url,
            credential=az_credential,
            file_system_name=passed_container_name,
            file_path=passed_file_path,
        )
        data = az_client.download_file().readall()
        decoded_data = data.decode("utf-8")
        return decoded_data
