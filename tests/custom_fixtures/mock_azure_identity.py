from unittest.mock import MagicMock
from ..custom_fixtures.mock_exceptions import MockClientAuthenticationError

mock_az_token = "mock_token"

mock_mi_credential = MagicMock()
mock_mi_credential_instance = mock_mi_credential.return_value
mock_mi_credential_instance.get_token.return_value = mock_az_token

# mock fail
class MockManagedIdentityCredentialFailCAE:
    def __init__(self):
        pass

    def get_token(self, url):
        raise MockClientAuthenticationError(
          "simulated error MockManagedIdentityCredentialFailCAE"
        )

class MockManagedIdentityCredentialFailE:
    def __iniot__(self):
        pass

    def get_token(self, url):
        raise Exception(
          "simulated error MockManagedIdentityCredentialFailE"
        )

