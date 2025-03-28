# pytest all possible test cases with monkeypatch for the below code

from azure.identity import ManagedIdentityCredential
from azure.storage.filedatalake import DataLakeFileClient

from pyspark.sql import SparkSession

from azure.core.exceptions import (
    ClientAuthenticationError, # if tenant_id, client_id or client_secret is incorrect
    ResourceNotFoundError, # cannot find the file on ADLS
)


class PreProcessingUtils:
    def __init__(self, lc, source_system_type, file_type_to_process, dbutils=None):
        self.lc = lc
        self.file_type_to_process = file_type_to_process
    
    def _get_az_credential(self) -> ManagedIdentityCredential:
        this_module = f"[{type(self).__name__}._get_az_credential()] -"
        try:
            az_credential = ManagedIdentityCredential()
            az_credential.get_token(
                "https://storage.azure.com/.default"
            )
            return az_credential
        except ClientAuthenticationError as e:
            print(f"{this_module} Cannot obtain the Azure access token to access ADLS ({e})")
            raise
        except Exception as e:
            print(f"{this_module} Error setting up ManagedIdentityCredential ({e})")
            raise
    
    def get_file_data(self, passed_account_url, passed_container_name, passed_file_path):
        this_module = f"[{type(self).__name__}.get_file_data()] -"
        if type(passed_account_url) is not str:
            error_msg = f"{this_module} TypeError : "
                        f"passed_account_url -- {passed_account_url} must be a string"
            print(error_msg)
            raise TypeError(error_msg)
        if passed_account_url.strip() == "":
            error_msg = f"{this_module} "
                        f"passed_account_url -- {passed_account_url} must not be an empty string"
            print(error_msg)
            raise Exception(error_msg)
        
        if type(passed_container_name) is not str:
            error_msg = f"{this_module} TypeError : "
                        f"passed_container_name -- {passed_container_name} must be a string"
            print(error_msg)
            raise TypeError(error_msg)
        if passed_container_name.strip() == "":
            error_msg = f"{this_module} "
                        f"passed_container_name -- {passed_container_name} must not be an empty string"
            print(error_msg)
            raise Exception(error_msg)
        
        if type(passed_file_path) is not str:
            error_msg = f"{this_module} TypeError : "
                        f"passed_file_path -- {passed_file_path} must be a string"
            print(error_msg)
            raise TypeError(error_msg)
        if passed_file_path.strip() == "":
            error_msg = f"{this_module} "
                        f"passed_file_path -- {passed_file_path} must not be an empty string"
            print(error_msg)
            raise Exception(error_msg)
        
        az_credential = self._get_az_credential()
        try:
            az_client = DataLakeFileClient(
                account_url=passed_account_url,
                credential=az_credential,
                file_system_name=passed_container_name,
                file_path=passed_file_path,
            )
            data = az_client.download_file().readall()
            decoded_data = data.decode("utf-8")
            return decoded_data
        except ResourceNotFoundError as e:
            print(f"{this_module} ({e})")
            raise
        except Exception as e:
            print(f"{this_module} ({e})")
            raise
