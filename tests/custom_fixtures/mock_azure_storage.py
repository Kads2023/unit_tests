from unittest.mock import MagicMock
from ..custom_fixtures.mock_exceptions import *

mock_file_content = b"mock_file_content"

mock_azure_storage_dldc = MagicMock()
mock_azure_storage_dldc_instance = mock_azure_storage_dldc.return_value
mock_azure_storage_dldc_instance.download_file.return_value.readall.return_value = mock_file_content


mock_azure_storage_dldc_fail = MagicMock()
mock_azure_storage_dldc_instance = (mock_azure_storage_dldc_fail.return_value)
mock_azure_storage_dldc_instance.download_file.return_value.readall.side_effect = Exception(
  "simulated error general Exception from DataLakeFileClient - Download failed"
)


mock_azure_storage_dldc_fail_rsrc = MagicMock()
mock_azure_storage_dldc_fail_rsrc_instance = (mock_azure_storage_dldc_fail_rsrc_fail.return_value)
mock_azure_storage_dldc_fail_rsrc_instance.download_file.return_value.readall.side_effect = MockResourceNotFoundError(
  "simulated error MockResourceNotFoundError from DataLakeFileClient - Download failed"
)


# class DataLakeFileClientMock:
#   download_file = MagicMock()

#   def __init__(
#     self, account_url, credential, file_system_name, file_path
#   ):
#     self.account_url = account_url
#     self.credential = credential
#     self.file_system_name = file_system_name
#     self.file_path = file_path
#     self.download_file.readall = self._readall()

#   def _readall(self):
#     return mock_file_content
