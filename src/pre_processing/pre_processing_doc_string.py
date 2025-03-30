from azure.identity import ManagedIdentityCredential
from azure.storage.filedatalake import DataLakeFileClient
from pyspark.sql import SparkSession
from pre_processing.default_values import *
import re
from azure.core.exceptions import (
    ClientAuthenticationError,  # if tenant_id, client_id or client_secret is incorrect
    ResourceNotFoundError,  # cannot find file on ADLS
)
from pre_processing.file_categories.file_category_factory import FileCategoriesFactory
from pre_processing.file_categories.base_file_categories import BaseFileCategories


class PreProcessingUtils:
    """
    A utility class for handling pre-processing tasks such as file retrieval, directory listing,
    and Azure authentication.
    """

    def __init__(self, lc, source_system_type: str, file_type_to_process: str, dbutils=None):
        """
        Initializes the PreProcessingUtils class.

        Args:
            lc: Logger class instance.
            source_system_type (str): The source system type.
            file_type_to_process (str): The type of file to process.
            dbutils: Optional Databricks utility class instance.

        Raises:
            TypeError: If `source_system_type` or `file_type_to_process` is not a string.
            Exception: If `source_system_type` or `file_type_to_process` is empty.
        """
        self.this_class_name = f"{type(self).__name__}"
        this_module = f"[{self.this_class_name}.__init__()] -"
        self.lc = lc
        self.ret_report_details_dict_list = []
        self.ret_report_details_dict = {}
        self.check_keys_that_exists = {}

        if not isinstance(source_system_type, str) or not source_system_type.strip():
            raise TypeError(f"{this_module} source_system_type must be a non-empty string")
        if not isinstance(file_type_to_process, str) or not file_type_to_process.strip():
            raise TypeError(f"{this_module} file_type_to_process must be a non-empty string")

        self.file_category_obj: BaseFileCategories = FileCategoriesFactory(lc).get_file_category_object(
            source_system_type)
        self.file_type_to_process = file_type_to_process

        if dbutils is not None:
            self.dbutils = dbutils
        else:
            try:
                from pyspark.dbutils import DBUtils
                self.dbutils = DBUtils(SparkSession.getActiveSession())
            except ModuleNotFoundError:
                self.dbutils = None

    def get_list_of_files_to_process(self):
        """
        Retrieves a list of files to be processed for the given file type.

        Returns:
            list: A list of file paths to process.
        """
        return self.file_category_obj.get_list_of_files_to_process(self.file_type_to_process)

    def get_list_of_directories(self, passed_file_location: str):
        """
        Retrieves a list of directories from the given file location.

        Args:
            passed_file_location (str): The file location path.

        Returns:
            list: A list of directory names.

        Raises:
            TypeError: If `passed_file_location` is not a string.
            Exception: If `passed_file_location` is empty.
            FileNotFoundError: If the specified file location does not exist.
        """
        this_module = f"[{self.this_class_name}.get_list_of_directories()] -"

        if not isinstance(passed_file_location, str) or not passed_file_location.strip():
            raise TypeError(f"{this_module} passed_file_location must be a non-empty string")

        try:
            return [entry.name for entry in self.dbutils.fs.ls(passed_file_location) if entry.isDir()]
        except FileNotFoundError as e:
            self.lc.logger.error(f"{this_module} ({e})")
            raise
        except Exception as e:
            self.lc.logger.error(f"{this_module} ({e})")
            raise

    def _get_az_credential(self) -> ManagedIdentityCredential:
        """
        Retrieves Azure Managed Identity credentials.

        Returns:
            ManagedIdentityCredential: The Azure credential instance.

        Raises:
            ClientAuthenticationError: If unable to obtain an Azure access token.
            Exception: If any other error occurs during authentication.
        """
        this_module = f"[{type(self).__name__}._get_az_credential()] -"
        try:
            az_credential = ManagedIdentityCredential()
            az_credential.get_token("https://storage.azure.com/.default")
            return az_credential
        except ClientAuthenticationError as e:
            self.lc.logger.error(f"{this_module} Cannot obtain the Azure access token to access ADLS ({e})")
            raise
        except Exception as e:
            self.lc.logger.error(f"{this_module} Error setting up ManagedIdentityCredential ({e})")
            raise

    def get_file_data(self, passed_account_url: str, passed_container_name: str, passed_file_path: str) -> str:
        """
        Retrieves file data from Azure Data Lake Storage.

        Args:
            passed_account_url (str): The Azure account URL.
            passed_container_name (str): The container name.
            passed_file_path (str): The file path in the container.

        Returns:
            str: The file contents as a UTF-8 decoded string.

        Raises:
            TypeError: If any input parameter is not a string.
            Exception: If any input string is empty.
            ResourceNotFoundError: If the specified file does not exist in ADLS.
        """
        this_module = f"[{type(self).__name__}.get_file_data()] -"

        for param, param_name in zip(
                [passed_account_url, passed_container_name, passed_file_path],
                ["passed_account_url", "passed_container_name", "passed_file_path"]
        ):
            if not isinstance(param, str) or not param.strip():
                raise TypeError(f"{this_module} {param_name} must be a non-empty string")

        az_credential = self._get_az_credential()
        try:
            az_client = DataLakeFileClient(
                account_url=passed_account_url,
                credential=az_credential,
                file_system_name=passed_container_name,
                file_path=passed_file_path,
            )
            return az_client.download_file().readall().decode("utf-8")
        except ResourceNotFoundError as e:
            self.lc.logger.error(f"{this_module} ({e})")
            raise
        except Exception as e:
            self.lc.logger.error(f"{this_module} ({e})")
            raise


    def replace_special_characters(self, passed_string):
        """
        Replaces special characters in the given string with underscores.

        Parameters:
            passed_string (str): The string to be processed.

        Returns:
            str: The processed string with special characters replaced.

        Raises:
            TypeError: If the input is not a string.
            Exception: If an error occurs during processing.
        """
        this_module = f"[{type(self).__name__}.get_file_details()] -"
        if type(passed_string) is not str:
            error_msg = (f"{this_module} TypeError : "
                         f"passed_string -- {passed_string} must be a string")
            self.lc.logger.error(error_msg)
            raise TypeError(error_msg)

        try:
            replaced_value = re.sub(
                '[ "% ,;{}()\n\t=/]', "_", passed_string.strip().replace('"', "")
            )
            self.lc.logger.info(
                f"passed_string --> {passed_string}, replaced_value --> {replaced_value}"
            )
            return replaced_value
        except Exception as e:
            self.lc.logger.error(f"{this_module} ({e})")
            raise


    def get_unique_key(self, passed_key, passed_suffix):
        """
        Generates a unique key by appending an incremented counter and a suffix.

        Parameters:
            passed_key (str): The base key.
            passed_suffix (str): The suffix to append to the key.

        Returns:
            str: A unique key.

        Raises:
            TypeError: If the input parameters are not strings.
            Exception: If an error occurs during key generation.
        """
        this_module = f"[{type(self).__name__}.get_file_details()] -"
        if type(passed_key) is not str or type(passed_suffix) is not str:
            error_msg = f"{this_module} TypeError : Input parameters must be strings."
            self.lc.logger.error(error_msg)
            raise TypeError(error_msg)

        try:
            if passed_key in self.check_keys_that_exists.keys():
                past_value = self.check_keys_that_exists[passed_key]
                new_value = int(past_value) + 1
            else:
                new_value = 1
            self.check_keys_that_exists[passed_key] = new_value
            return f"{passed_key}_{new_value}_details{passed_suffix}"
        except Exception as e:
            self.lc.logger.error(f"{this_module} ({e})")
            raise


    def append_to_report_details_dict(self, passed_key, passed_value, passed_suffix):
        """
        Appends a key-value pair to the report details dictionary with a unique key.

        Parameters:
            passed_key (str): The base key.
            passed_value (str): The value to be stored.
            passed_suffix (str): The suffix to append to the key.

        Raises:
            TypeError: If input parameters are not strings.
            Exception: If an error occurs during dictionary update.
        """
        this_module = f"[{type(self).__name__}.append_to_report_details_dict()] -"
        if not all(isinstance(arg, str) for arg in [passed_key, passed_value, passed_suffix]):
            error_msg = f"{this_module} TypeError: All parameters must be strings."
            self.lc.logger.error(error_msg)
            raise TypeError(error_msg)

        try:
            new_key = self.get_unique_key(passed_key, passed_suffix) \
                if passed_key in self.ret_report_details_dict.keys() \
                else f"{passed_key}_details{passed_suffix}"
            self.lc.logger.info(
                f"Adding key: {new_key}, value: {passed_value}"
            )
            self.ret_report_details_dict[new_key] = passed_value
        except Exception as e:
            self.lc.logger.error(f"{this_module} ({e})")
            raise


    def check_and_append_to_report_details_dict(
            self, passed_key, passed_value, passed_suffix, passed_list_of_keys_to_add
    ):
        """
        Checks if a key should be appended to the report details dictionary based on a list.

        Parameters:
            passed_key (str): The key to check and add.
            passed_value (str): The value associated with the key.
            passed_suffix (str): The suffix to append to the key.
            passed_list_of_keys_to_add (list): A list of keys that should be added.

        Raises:
            TypeError: If input parameters are of incorrect types.
            Exception: If an error occurs during key validation or addition.
        """
        this_module = f"[{type(self).__name__}.append_to_report_details_dict()] -"
        if not all(isinstance(arg, str) for arg in [passed_key, passed_value, passed_suffix]):
            error_msg = f"{this_module} TypeError: passed_key, passed_value, and passed_suffix must be strings."
            self.lc.logger.error(error_msg)
            raise TypeError(error_msg)

        if not isinstance(passed_list_of_keys_to_add, list):
            error_msg = f"{this_module} TypeError: passed_list_of_keys_to_add must be a list."
            self.lc.logger.error(error_msg)
            raise TypeError(error_msg)

        try:
            final_key = self.replace_special_characters(passed_key)
            final_value = passed_value.strip().replace('"', "")
            add_key = False

            if len(passed_list_of_keys_to_add) == 0:
                self.lc.logger.info("No keys to add.")
            elif len(passed_list_of_keys_to_add) == 1:
                value_to_check = passed_list_of_keys_to_add[0].lower().strip()
                if value_to_check == "all":
                    add_key = True
                elif value_to_check in ["none", "nil", "no", ""]:
                    add_key = False
                else:
                    add_key = passed_key in passed_list_of_keys_to_add
            else:
                add_key = passed_key in passed_list_of_keys_to_add

            if add_key:
                self.append_to_report_details_dict(final_key, final_value, passed_suffix)
        except Exception as e:
            self.lc.logger.error(f"{this_module} ({e})")
            raise
