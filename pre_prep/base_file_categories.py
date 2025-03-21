from abc import abstractmethod


class BaseFileCategories:
  dict_of_file_type_wise_list_of_files_to_process = []
  dict_of_file_type_wise_file_columns_and_screen = {}

  @abstractmethod
  def __init__(self, lc):
    pass

  @abstractmethod
  def get_list_of_files_to_process(self, passed_file_type_to_process):
    pass

  @abstractmethod
  def get_file_columns_and_schema_dict(self, passed_file_type_to_process):
    pass
