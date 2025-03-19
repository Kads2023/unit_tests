import pytest
from unittest.mock import MagicMock
from azure.core.exceptions import ClientAuthenticationError
from your_module import PreProcessingUtils  # Replace with the actual module name


# Test for the _get_az_credential method
def test_get_az_credential_success(monkeypatch):
    # Mock the ManagedIdentityCredential class
    mock_credential = MagicMock()
    mock_credential.get_token.return_value = 'mock_token'

    def mock_managed_identity_credential():
        return mock_credential

    # Monkeypatch the ManagedIdentityCredential to use the mock
    monkeypatch.setattr('your_module.ManagedIdentityCredential', mock_managed_identity_credential)

    utils = PreProcessingUtils()
    result = utils._get_az_credential()

    # Assert that the get_token method was called with the expected URL
    mock_credential.get_token.assert_called_once_with("https://storage.azure.com/.default")
    assert result == mock_credential


def test_get_az_credential_auth_error(monkeypatch):
    # Simulate an authentication error
    def mock_managed_identity_credential():
        raise ClientAuthenticationError("Auth Error")

    # Monkeypatch the ManagedIdentityCredential to simulate an error
    monkeypatch.setattr('your_module.ManagedIdentityCredential', mock_managed_identity_credential)

    utils = PreProcessingUtils()

    with pytest.raises(ClientAuthenticationError):
        utils._get_az_credential()


# Test for the get_file_data method
def test_get_file_data_success(monkeypatch):
    # Mock the ManagedIdentityCredential and DataLakeFileClient classes
    mock_credential = MagicMock()
    mock_credential.get_token.return_value = 'mock_token'

    mock_client = MagicMock()
    mock_client.download_file.return_value.readall.return_value = b"mock_file_content"

    # Monkeypatch to replace the actual class constructors
    def mock_managed_identity_credential():
        return mock_credential

    def mock_data_lake_file_client(account_url, credential, file_system_name, file_path):
        return mock_client

    # Use monkeypatch to replace the class constructors
    monkeypatch.setattr('your_module.ManagedIdentityCredential', mock_managed_identity_credential)
    monkeypatch.setattr('your_module.DataLakeFileClient', mock_data_lake_file_client)

    # Sample input
    passed_account_url = "https://mockaccount.dfs.core.windows.net"
    passed_container_name = "mockcontainer"
    passed_file_path = "mockfile.txt"

    utils = PreProcessingUtils()
    result = utils.get_file_data(passed_account_url, passed_container_name, passed_file_path)

    # Check that the download_file was called
    mock_client.download_file.assert_called_once()

    # Assert the returned result matches the expected decoded content
    assert result == "mock_file_content"


# Test for invalid input types
def test_get_file_data_invalid_input():
    utils = PreProcessingUtils()

    # Test with invalid input types
    with pytest.raises(TypeError):
        utils.get_file_data(123, "mockcontainer", "mockfile.txt")

    with pytest.raises(TypeError):
        utils.get_file_data("mockaccount", 123, "mockfile.txt")

    with pytest.raises(TypeError):
        utils.get_file_data("mockaccount", "mockcontainer", 123)


# Test for _get_az_credential method with general exception handling
def test_get_az_credential_general_error(monkeypatch):
    # Simulate a general exception during credential setup
    def mock_managed_identity_credential():
        raise Exception("General error")

    # Monkeypatch the ManagedIdentityCredential to simulate an error
    monkeypatch.setattr('your_module.ManagedIdentityCredential', mock_managed_identity_credential)

    utils = PreProcessingUtils()

    with pytest.raises(Exception):
        utils._get_az_credential()


# Test when managed identity credential is not set up correctly and get_token fails
def test_get_az_credential_token_failure(monkeypatch):
    # Mock the ManagedIdentityCredential to raise an exception on get_token
    mock_credential = MagicMock()
    mock_credential.get_token.side_effect = Exception("Token retrieval failed")

    def mock_managed_identity_credential():
        return mock_credential

    # Monkeypatch the ManagedIdentityCredential to use the mock
    monkeypatch.setattr('your_module.ManagedIdentityCredential', mock_managed_identity_credential)

    utils = PreProcessingUtils()

    with pytest.raises(Exception, match="Token retrieval failed"):
        utils._get_az_credential()


# Test for DataLakeFileClient when download_file fails
def test_get_file_data_download_fail(monkeypatch):
    # Mock the ManagedIdentityCredential and DataLakeFileClient classes
    mock_credential = MagicMock()
    mock_credential.get_token.return_value = 'mock_token'

    mock_client = MagicMock()
    # Simulate download_file failure
    mock_client.download_file.return_value.readall.side_effect = Exception("Download failed")

    # Monkeypatch to replace the actual class constructors
    def mock_managed_identity_credential():
        return mock_credential

    def mock_data_lake_file_client(account_url, credential, file_system_name, file_path):
        return mock_client

    # Use monkeypatch to replace the class constructors
    monkeypatch.setattr('your_module.ManagedIdentityCredential', mock_managed_identity_credential)
    monkeypatch.setattr('your_module.DataLakeFileClient', mock_data_lake_file_client)

    # Sample input
    passed_account_url = "https://mockaccount.dfs.core.windows.net"
    passed_container_name = "mockcontainer"
    passed_file_path = "mockfile.txt"

    utils = PreProcessingUtils()

    with pytest.raises(Exception, match="Download failed"):
        utils.get_file_data(passed_account_url, passed_container_name, passed_file_path)
# Explanation of the Tests:
# test_get_az_credential_success:

# This test ensures that when the ManagedIdentityCredential is correctly set up, the get_token method is called with the correct parameters and returns the expected credential.
# test_get_az_credential_auth_error:

# This test simulates a scenario where obtaining the token fails due to authentication issues. We expect a ClientAuthenticationError to be raised.
# test_get_file_data_success:

# This test mocks both the credential and the DataLake client to simulate a successful file download from Azure. It verifies that the get_file_data method properly retrieves and decodes the file data.
# test_get_file_data_invalid_input:

# This test checks that the get_file_data method raises a TypeError when one or more of the input parameters are not of type str.
# test_get_az_credential_general_error:

# This test simulates a general error (e.g., a problem with setting up the credential) and ensures that the method raises a generic exception when the credential setup fails.
# test_get_az_credential_token_failure:

# This test checks that if the get_token method fails (e.g., due to a network issue or misconfiguration), the method raises an exception.
# test_get_file_data_download_fail:

# This test ensures that if the file download fails (e.g., network issues, missing file), an exception is raised.
# Running the Tests:
# To run these tests using pytest, execute the following command in your terminal:

# bash
# Copy
# pytest test_your_module.py
