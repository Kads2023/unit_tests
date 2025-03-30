from azure.identity import ManagedIdentityCredential
from azure.storage.filedatalake import DataLakeFileClient

from pyspark.sql import SparkSession

from pre_processing.default_values import *

import re

from azure.core.exceptions import (
    ClientAuthenticationError, # if tenant_id, client_id or client_secret is incorrect
    ResourceNotFoundError, # cannot find file on ADLS
)

from pre_processing.file_categories.file_category_factory import FileCategoriesFactory
from pre_processing.file_categories.base_file_categories import BaseFileCategories


class PreProcessingUtils:

    def __init__(self, lc, source_system_type, file_type_to_process, dbutils=None):
        self.this_class_name = f"{type(self).__name__}"
        this_module = f"[{self.this_class_name}.__init__()] -"
        self.lc = lc
        self.ret_report_details_dict_list = []
        self.ret_report_details_dict = {}
        self.check_keys_that_exists = {}

        if type(source_system_type) is not str:
            error_msg = (
                f"{this_module} TypeError: "
                f"source_system_type -- {source_system_type}, "
                f"must be a string"
            )
            self.lc.logger.error(error_msg)
            raise TypeError(error_msg)
        if source_system_type.strip() == "":
            error_msg = (
                f"{this_module} TypeError: "
                f"source_system_type -- {source_system_type}, "
                f"must not be an empty string"
            )
            self.lc.logger.error(error_msg)
            raise Exception(error_msg)

        if type(file_type_to_process) is not str:
            error_msg = (
                f"{this_module} TypeError: "
                f"file_type_to_process -- {file_type_to_process}, "
                f"must be a string"
            )
            self.lc.logger.error(error_msg)
            raise TypeError(error_msg)
        if file_type_to_process.strip() == "":
            error_msg = (
                f"{this_module} TypeError: "
                f"file_type_to_process -- {file_type_to_process}, "
                f"must not be an empty string"
            )
            self.lc.logger.error(error_msg)
            raise Exception(error_msg)

        self.file_category_obj: BaseFileCategories = FileCategoriesFactory(
            lc
        ).get_file_category_object(source_system_type)
        self.file_type_to_process = file_type_to_process

        if dbutils is not None:
            self.dbutils = dbutils
        else:
            spark = SparkSession.getActiveSession()
            try:
                from pyspark.dbutils import DBUtils

                self.dbutils = DBUtils(spark)
            except ModuleNotFoundError:
                # NOTE: if dbutils functionality required outside of databricks,
                # either it must be mocked in DBUtilsMock OR
                # a way to import dbutils must be discovered
                self.dbutils = None

    def get_list_of_files_to_process(self):
        return self.file_category_obj.get_list_of_files_to_process(
            self.file_type_to_process
        )

    def get_list_of_directories(self, passed_file_location):
        this_module = f"[{self.this_class_name}.get_list_of_directories()] -"
        if type(passed_file_location) is not str:
            error_msg = (
                f"{this_module} TypeError: "
                f"passed_file_location -- {passed_file_location}, "
                f"must be a string"
            )
            self.lc.logger.error(error_msg)
            raise TypeError(error_msg)
        if passed_file_location.strip() == "":
            error_msg = (
                f"{this_module} TypeError: "
                f"passed_file_location -- {passed_file_location}, "
                f"must not be an empty string"
            )
            self.lc.logger.error(error_msg)
            raise Exception(error_msg)

        try:
            dir_list = []
            list_of_folders = self.dbutils.fs.ls(passed_file_location)
            for each_listing in list_of_folders:
                each_listing_name = each_listing.name
                if each_listing.isDir():
                    dir_list.append(each_listing_name)
            return dir_list
        except FileNotFoundError as e:
            self.lc.logger.error(f"{this_module} ({e})")
            raise
        except Exception as e:
            self.lc.logger.error(f"{this_module} ({e})")
            raise

    def get_dict_of_headers_and_strings_to_compare(self):
        this_module = f"[{self.this_class_name}.get_dict_of_headers_and_strings_to_compare()] -"
        try:
            file_columns_and_schema = (
                self.file_category_obj.get_file_columns_and_schema_dict(
                    self.file_type_to_process
                )
            )
            no_of_sections = len(file_columns_and_schema.keys())
            strings_to_compare_and_sections_dict = {}
            section_header_dict = {}
            for each_section, each_section_values in file_columns_and_schema.items():
                each_header = each_section_values["header"]
                each_string_to_compare = each_section_values["string_to_compare"]
                strings_to_compare_and_sections_dict[each_string_to_compare] = each_section
                new_header = f"{each_header}{default_added_header_columns}"
                section_header_dict[each_section] = new_header
                return no_of_sections, strings_to_compare_and_sections_dict, section_header_dict
        except Exception as e:
            self.lc.logger.error(f"{this_module} ({e})")
            raise

    def _get_az_credential(self) -> ManagedIdentityCredential:
        this_module = f"[{type(self).__name__}._get_az_credential()] -"
        try:
            az_credential = ManagedIdentityCredential()
            az_credential.get_token(
                "https://storage.azure.com/.default"
            )
            return az_credential
        except ClientAuthenticationError as e:
            self.lc.logger.error(f"{this_module} Cannot obtain the Azure access token to access ADLS ({e})")
            raise
        except Exception as e:
            self.lc.logger.error(f"{this_module} Error setting up ManagedIdentityCredential ({e})")
            raise

    def get_file_data(self, passed_account_url, passed_container_name, passed_file_path):
        this_module = f"[{type(self).__name__}.get_file_data()] -"
        if type(passed_account_url) is not str:
            error_msg = (f"{this_module} TypeError : "
                         f"passed_account_url -- {passed_account_url} must be a string")
            self.lc.logger.error(error_msg)
            raise TypeError(error_msg)
        if passed_account_url.strip() == "":
            error_msg = (f"{this_module} "
                         f"passed_account_url -- {passed_account_url} must not be an empty string")
            self.lc.logger.error(error_msg)
            raise Exception(error_msg)

        if type(passed_container_name) is not str:
            error_msg = (f"{this_module} TypeError : "
                         f"passed_container_name -- {passed_container_name} must be a string")
            self.lc.logger.error(error_msg)
            raise TypeError(error_msg)
        if passed_container_name.strip() == "":
            error_msg = (f"{this_module} "
                         f"passed_container_name -- {passed_container_name} must not be an empty string")
            self.lc.logger.error(error_msg)
            raise Exception(error_msg)

        if type(passed_file_path) is not str:
            error_msg = (f"{this_module} TypeError : "
                         f"passed_file_path -- {passed_file_path} must be a string")
            self.lc.logger.error(error_msg)
            raise TypeError(error_msg)
        if passed_file_path.strip() == "":
            error_msg = (f"{this_module} "
                         f"passed_file_path -- {passed_file_path} must not be an empty string")
            self.lc.logger.error(error_msg)
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
            self.lc.logger.error(f"{this_module} ({e})")
            raise
        except Exception as e:
            self.lc.logger.error(f"{this_module} ({e})")
            raise

    def get_file_details(self, passed_file_name, passed_folder_name):
        this_module = f"[{type(self).__name__}.get_file_details()] -"
        if type(passed_file_name) is not str:
            error_msg = (f"{this_module} TypeError : "
                         f"passed_file_name -- {passed_file_name} must be a string")
            self.lc.logger.error(error_msg)
            raise TypeError(error_msg)
        if passed_file_name.strip() == "":
            error_msg = (f"{this_module} "
                         f"passed_file_name -- {passed_file_name} must not be an empty string")
            self.lc.logger.error(error_msg)
            raise Exception(error_msg)

        if type(passed_folder_name) is not str:
            error_msg = (f"{this_module} TypeError : "
                         f"passed_folder_name -- {passed_folder_name} must be a string")
            self.lc.logger.error(error_msg)
            raise TypeError(error_msg)
        if passed_folder_name.strip() == "":
            error_msg = (f"{this_module} "
                         f"passed_folder_name -- {passed_folder_name} must not be an empty string")
            self.lc.logger.error(error_msg)
            raise Exception(error_msg)

        try:
            file_name_split_list = passed_file_name.replace(".csv", "").split("_")
            length_file_name_split_list = len(file_name_split_list)
            self.lc.logger.info(
                f"file_name_split_list --> {file_name_split_list}, "
                f"length_file_name_split_list --> {length_file_name_split_list}"
            )
            if length_file_name_split_list == 1:
                file_prefix = file_name_split_list[0]
                file_suffix = file_prefix
                file_region = file_suffix
            elif length_file_name_split_list == 2:
                file_prefix = file_name_split_list[0]
                file_suffix = file_name_split_list[1]
                file_region = file_suffix
            elif length_file_name_split_list > 2:
                file_prefix = file_name_split_list[0]
                file_suffix = file_name_split_list[1]
                file_region = file_name_split_list[2]
            else:
                error_msg = (f"{this_module} "
                             f"file_name_split_list --> {file_name_split_list}, "
                             f"length_file_name_split_list --> {length_file_name_split_list}")
                self.lc.logger.error(error_msg)
                raise Exception(error_msg)
            ret_file_details_dict = {
                "folder_name": passed_folder_name,
                "file_name": passed_file_name,
                "file_prefix": file_prefix,
                "file_region": file_region,
                "file_suffix": file_suffix,
            }
            return ret_file_details_dict
        except Exception as e:
            self.lc.logger.error(f"{this_module} ({e})")
            raise

    def replace_special_characters(self, passed_string):
        this_module = f"[{type(self).__name__}.get_file_details()] -"
        if type(passed_string) is not str:
            error_msg = (f"{this_module} TypeError : "
                         f"passed_string -- {passed_string} must be a string")
            self.lc.logger.error(error_msg)
            raise TypeError(error_msg)

        try:
            replaced_value = re.sub(
                '[ "%,;{}()\n\t=/]', "_", passed_string.strip().replace('"', "")
            )
            self.lc.logger.info(
                f"passed_string --> {passed_string}, replaced_value --> {replaced_value}"
            )
            return replaced_value
        except Exception as e:
            self.lc.logger.error(f"{this_module} ({e})")
            raise

    def get_unique_key(self, passed_key, passed_suffix):
        this_module = f"[{type(self).__name__}.get_file_details()] -"
        if type(passed_key) is not str:
            error_msg = (f"{this_module} TypeError : "
                         f"passed_key -- {passed_key} must be a string")
            self.lc.logger.error(error_msg)
            raise TypeError(error_msg)

        if type(passed_suffix) is not str:
            error_msg = (f"{this_module} TypeError : "
                         f"passed_suffix -- {passed_suffix} must be a string")
            self.lc.logger.error(error_msg)
            raise TypeError(error_msg)

        try:
            if passed_key in self.check_keys_that_exists.keys():
                past_value = self.check_keys_that_exists[passed_key]
                new_value = int(past_value) + 1
            else:
                new_value = 1
            self.check_keys_that_exists[passed_key] = new_value
            ret_key = f"{passed_key}_{new_value}_details{passed_suffix}"
            return ret_key
        except Exception as e:
            self.lc.logger.error(f"{this_module} ({e})")
            raise

    def append_to_report_details_dict(self, passed_key, passed_value, passed_suffix):
        this_module = f"[{type(self).__name__}.append_to_report_details_dict()] -"
        if type(passed_key) is not str:
            error_msg = (f"{this_module} TypeError : "
                         f"passed_key -- {passed_key} must be a string")
            self.lc.logger.error(error_msg)
            raise TypeError(error_msg)

        if type(passed_value) is not str:
            error_msg = (f"{this_module} TypeError : "
                         f"passed_value -- {passed_value} must be a string")
            self.lc.logger.error(error_msg)
            raise TypeError(error_msg)

        if type(passed_suffix) is not str:
            error_msg = (f"{this_module} TypeError : "
                         f"passed_suffix -- {passed_suffix} must be a string")
            self.lc.logger.error(error_msg)
            raise TypeError(error_msg)

        try:
            if passed_key in self.ret_report_details_dict.keys():
                new_key = self.get_unique_key(passed_key, passed_suffix)
            else:
                new_key = f"{passed_key}_details{passed_suffix}"
            self.lc.logger.info(
                f"passed_key --> {passed_key},passed_suffix --> {passed_suffix},"
                f"new_key--> {new_key},passed_value --> {passed_value}"

            )
            self.ret_report_details_dict[new_key] = passed_value
            self.lc.logger.info(
                f"ret_report_details_dict --> {self.ret_report_details_dict}"
            )
        except Exception as e:
            self.lc.logger.error(f"{this_module} ({e})")
            raise

    def check_and_append_to_report_details_dict(
            self,
            passed_key,
            passed_value,
            passed_suffix,
            passed_list_of_keys_to_add,
    ):
        this_module = f"[{type(self).__name__}.append_to_report_details_dict()] -"
        if type(passed_key) is not str:
            error_msg = (f"{this_module} TypeError : "
                         f"passed_key -- {passed_key} must be a string")
            self.lc.logger.error(error_msg)
            raise TypeError(error_msg)

        if type(passed_value) is not str:
            error_msg = (f"{this_module} TypeError : "
                         f"passed_value -- {passed_value} must be a string")
            self.lc.logger.error(error_msg)
            raise TypeError(error_msg)

        if type(passed_suffix) is not str:
            error_msg = (f"{this_module} TypeError : "
                         f"passed_suffix -- {passed_suffix} must be a string")
            self.lc.logger.error(error_msg)
            raise TypeError(error_msg)

        if type(passed_list_of_keys_to_add) is not list:
            error_msg = (f"{this_module} TypeError : "
                         f"passed_suffix -- {passed_list_of_keys_to_add} must be a list")
            self.lc.logger.error(error_msg)
            raise TypeError(error_msg)

        try:
            final_key = self.replace_special_characters(passed_key)
            final_value = passed_value.strip().replace('"',"")
            self.lc.logger.info(
                f"passed_key-->{passed_key}, "
                f"passed_value --> {passed_value}, "
                f"final_key --> {final_key}, "
                f"final_value --> {final_value}"
            )
            add_key = False
            if len(passed_list_of_keys_to_add) == 0:
                self.lc.logger.info(
                    f"passed_key --> {passed_key},"
                    f"passed_list_of_keys_to_add --> {passed_list_of_keys_to_add},"
                    f"is empty and doesn't not have all, hence not adding any keys"
                )
            elif len(passed_list_of_keys_to_add) >= 1:
                check_keys = True
                if len(passed_list_of_keys_to_add) == 1:
                    check_keys = False
                    value_to_check = passed_list_of_keys_to_add[0].lower().strip()
                    if value_to_check == "all":
                        self.lc.logger.info(
                            f"passed_key --> {passed_key},"
                            f"passed_list_of_keys_to_add --> "
                            f"{passed_list_of_keys_to_add}, "
                            f"starts with all, hence adding the key"
                        )
                        add_key = True
                    elif(
                        value_to_check == "none"
                        or value_to_check == "nil"
                        or value_to_check == "no"
                        or value_to_check == ""

                    ):
                        self.lc.logger.info(
                            f"passed_key --> {passed_key},"
                            f"passed_list_of_keys_to_add --> "
                            f"{passed_list_of_keys_to_add}, "
                            f"starts with none, hence not adding the key"
                        )
                    else:
                        check_keys = True
                if check_keys:
                    if passed_key in passed_list_of_keys_to_add:
                        self.lc.logger.info(
                            f"passed_key --> {passed_key},in,"
                            f"passed_list_of_keys_to_add --> "
                            f"{passed_list_of_keys_to_add}, "
                            f"hence adding the key"
                        )
                        add_key = True
                    else:
                        self.lc.logger.info(
                            f"passed_key --> {passed_key},not in"
                            f"passed_list_of_keys_to_add --> "
                            f"{passed_list_of_keys_to_add}, " 
                            f"hence not adding the key"
                        )
            if add_key:
                self.append_to_report_details_dict(
                    final_key, final_value, passed_suffix
                )
        except Exception as e:
            self.lc.logger.error(f"{this_module} ({e})")
            raise

    def collate_report_details_dics(
            self,passed_split_line_list, passed_split_line_length,
            passed_line_counter, passed_file_details_dict,
            passed_reset_report_dict_line_no, passed_suffix,
            passed_list_of_keys_to_add):
        this_module = f"[{self.this_class_name}.collate_report_details_dict()] -"
        if type(passed_split_line_list) is not list:
            error_msg = (
                f"{this_module} TypeError: "
                f"passed_split_line_list -- {passed_split_line_list} "
                f"must be a list"
            )
            self.lc.loger.error(error_msg)
            raise TypeError(error_msg)

        if type(passed_split_line_length) is not int:
            error_msg = (
                f"{this_module} TypeError: "
                f"passed_split_line_length -- {passed_split_line_length} "
                f"must be an integer"

            )
            self.lc.loger.error(error_msg)
            raise TypeError(error_msg)

        if type(passed_line_counter) is not int:
            error_msg = (
                f"{this_module} TypeError: "
                f"passed_line_counter -- {passed_line_counter} "
                f"must be an integer"

            )
            self.lc.loger.error(error_msg)
            raise TypeError(error_msg)

        if type(passed_file_details_dict) is not dict:
            error_msg = (
                f"{this_module} TypeError: "
                f"passed_file_details_dict -- {passed_file_details_dict} "
                f"must be a dictionary"

            )
            self.lc.loger.error(error_msg)
            raise TypeError(error_msg)

        if type(passed_reset_report_dict_line_no) is not int:
            error_msg = (
                f"{this_module} TypeError: "
                f"passed_reset_report_dict_line_no -- {passed_reset_report_dict_line_no} "
                f"must be an integer"

            )
            self.lc.loger.error(error_msg)
            raise TypeError(error_msg)

        if type(passed_suffix) is not str:
            error_msg = (
                f"{this_module} TypeError: "
                f"passed_suffix -- {passed_suffix} "
                f"must be a string"

            )
            self.lc.loger.error(error_msg)
            raise TypeError(error_msg)

        if type(passed_list_of_keys_to_add) is not list:
            error_msg = (
                f"{this_module} TypeError: "
                f"passed_list_of_keys_to_add -- {passed_list_of_keys_to_add} "
                f"must be a list"

            )
            self.lc.loger.error(error_msg)
            raise TypeError(error_msg)

        actual_line_counter = passed_line_counter
