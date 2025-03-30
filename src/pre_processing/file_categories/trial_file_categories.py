from .base_file_categories import BaseFileCategories


class TrialFileCategories(BaseFileCategories):
    dict_of_file_type_wise_list_of_files_to_process = {'trial_1': ['trial.csv']}
    dict_of_file_type_wise_file_columns_and_schema = {
      'trial_1': {
        "data": {
          "header": "Type,Term",
          "string_to_compare": '"Type","Term"'
        },
        "data_currency_basis": {
          "header": "Basis_Type,Basis_Term,Basis_Offer",
          "string_to_compare": '"Type","Term","Basis Offer"'
        }
      }
    }
    list_of_files_keys = set(
        dict_of_file_type_wise_list_of_files_to_process.keys()
    )
    file_columns_and_schema_keys = set(
        dict_of_file_type_wise_file_columns_and_schema.keys()
    )
  
    def __init__(self, lc):
        self.lc = lc
        self.this_class_name = f"{type(self).__name__}"
        if self.list_of_files_keys != self.file_columns_and_schema_keys:
            self.file_types = []
            error_msg = (f"[{self.this_class_name}.__init__()] - "
                         f"keys do not match, "
                         f"check class implementation, "
                         f"list_of_files_keys --> {self.list_of_files_keys}, "
                         f"file_columns_and_schema_keys --> "
                         f"{self.file_columns_and_schema_keys}"
                        )
            print(error_msg)
            raise Exception(error_msg)
        else:
            self.file_types = list(self.list_of_files_keys)
  
    def get_list_of_files_to_process(self, passed_file_type_to_process):
        if passed_file_type_to_process in self.file_types:
            return self.dict_of_file_type_wise_list_of_files_to_process[passed_file_type_to_process]
        else:
            raise Exception(f"[{self.this_class_name}.get_list_of_files_to_process()] - "
                            f"{passed_file_type_to_process} not available in "
                            f"files_to_process dictionary, "
                            f"keys available --> {self.file_types}"
                           )
  
    def get_file_columns_and_schema_dict(self, passed_file_type_to_process):
        if passed_file_type_to_process in self.file_types:
            return self.dict_of_file_type_wise_file_columns_and_schema[passed_file_type_to_process]
        else:
            raise Exception(f"[{self.this_class_name}.get_list_of_files_to_process()] - "
                            f"{passed_file_type_to_process} not available in "
                            f"file_columns_and_schema dictionary, "
                            f"keys available --> {self.file_types}"
                           )


# from .base_file_categories import BaseFileCategories
#
#
# class TrialFileCategories(BaseFileCategories):
#     """
#     A class that defines file categories for trial processing.
#
#     This class extends BaseFileCategories and provides file lists
#     and schemas for different trial file types.
#     """
#
#     dict_of_file_type_wise_list_of_files_to_process = {'trial_1': ['trial.csv']}
#     """
#     Dictionary mapping file types to lists of files that need processing.
#     """
#
#     dict_of_file_type_wise_file_columns_and_schema = {
#         'trial_1': {
#             "data": {
#                 "header": "Type,Term",
#                 "string_to_compare": '"Type","Term"'
#             },
#             "data_currency_basis": {
#                 "header": "Basis_Type,Basis_Term,Basis_Offer",
#                 "string_to_compare": '"Type","Term","Basis Offer"'
#             }
#         }
#     }
#     """
#     Dictionary mapping file types to their respective column headers and schema.
#     """
#
#     list_of_files_keys = set(dict_of_file_type_wise_list_of_files_to_process.keys())
#     """
#     Set of available file types for processing.
#     """
#
#     file_columns_and_schema_keys = set(dict_of_file_type_wise_file_columns_and_schema.keys())
#     """
#     Set of available file types for which column schemas are defined.
#     """
#
#     def __init__(self, lc):
#         """
#         Initializes the TrialFileCategories instance.
#
#         Parameters:
#         lc: Configuration or context parameter required for initialization.
#
#         Raises:
#         Exception: If the keys in the file list dictionary and schema dictionary do not match.
#         """
#         self.lc = lc
#         self.this_class_name = f"{type(self).__name__}"
#         if self.list_of_files_keys != self.file_columns_and_schema_keys:
#             self.file_types = []
#             error_msg = (f"[{self.this_class_name}.__init__()] - "
#                          f"keys do not match, "
#                          f"check class implementation, "
#                          f"list_of_files_keys --> {self.list_of_files_keys}, "
#                          f"file_columns_and_schema_keys --> "
#                          f"{self.file_columns_and_schema_keys}")
#             print(error_msg)
#             raise Exception(error_msg)
#         else:
#             self.file_types = list(self.list_of_files_keys)
#
#     def get_list_of_files_to_process(self, passed_file_type_to_process):
#         """
#         Retrieves the list of files to process for a given file type.
#
#         Parameters:
#         passed_file_type_to_process (str): The type of file to process.
#
#         Returns:
#         list: A list of files associated with the specified file type.
#
#         Raises:
#         Exception: If the specified file type is not available.
#         """
#         if passed_file_type_to_process in self.file_types:
#             return self.dict_of_file_type_wise_list_of_files_to_process[passed_file_type_to_process]
#         else:
#             raise Exception(f"[{self.this_class_name}.get_list_of_files_to_process()] - "
#                             f"{passed_file_type_to_process} not available in "
#                             f"files_to_process dictionary, "
#                             f"keys available --> {self.file_types}")
#
#     def get_file_columns_and_schema_dict(self, passed_file_type_to_process):
#         """
#         Retrieves the column schema for a given file type.
#
#         Parameters:
#         passed_file_type_to_process (str): The type of file whose schema is needed.
#
#         Returns:
#         dict: A dictionary containing the column headers and schema.
#
#         Raises:
#         Exception: If the specified file type is not available.
#         """
#         if passed_file_type_to_process in self.file_types:
#             return self.dict_of_file_type_wise_file_columns_and_schema[passed_file_type_to_process]
#         else:
#             raise Exception(f"[{self.this_class_name}.get_file_columns_and_schema_dict()] - "
#                             f"{passed_file_type_to_process} not available in "
#                             f"file_columns_and_schema dictionary, "
#                             f"keys available --> {self.file_types}")
