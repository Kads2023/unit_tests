from pre_prep.file_categories.base_file_categories import BaseFileCategories


class MockFileCategoriesFactory:
  def __init__(self, lc):
    self.lc = lc
    self.this_class_name = f"{type(self).__name__}"

  de get_file_category_object(self, passed_source_system_type) -> BBaseFileCategories:
    raise Exception(
      f"[{self.this_class_name}.get_file_category_object()] - "
      f"Mock Exception"
    )


class MockBaseFileCategories(BaseFileCategories):
    def __init__(self, lc):
        self.lc = lc
  
    def get_list_of_files_to_process(self, passed_file_type_to_process):
        return ['mock_file.csv]
  
    def get_file_columns_and_schema_dict(self, passed_file_type_to_process):
        return {
          "data": {
            "header": "Type,Term",
            "string_to_compare": '"Type","Term"'
          }
        }
  

class MockBaseFileCategoriesE(BaseFileCategories):
    def __init__(self, lc):
        self.lc = lc
        self.this_class_name = f"{type(self).__name__}"
        raise Exception(
          f"[{self.this_class_name}.__init__()] - "
          f"keys do not match, "
          f"check class implementation, "
          f"list_of_files_keys")
  
    def get_list_of_files_to_process(self, passed_file_type_to_process):
      raise Exception(
        f"[{self.this_class_name}.get_list_of_files_to_process()] - "
        f"{passed_file_type_to_process} "
        f"not available as key in files_to_process dictionary"
      )

    def get_file_columns_and_schema_dict(self, passed_file_type_to_process):
      raise Exception(
        f"[{self.this_class_name}.get_file_columns_and_schema_dict()] - "
        f"{passed_file_type_to_process} "
        f"not available as key in file_columns_and_schema dictionary"
      )

class MockBaseFileCategoriesKeyE(BaseFileCategories):
    dict_of_file_type_wise_list_of_files_to_process = {
      'trial_1': ['trial.csv']
    }
    dict_of_file_type_wise_file_columns_and_schema = {
      'trial_1': {},
      'trial': {}
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
