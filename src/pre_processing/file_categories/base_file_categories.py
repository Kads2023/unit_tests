from abc import abstractmethod


class BaseFileCategories:
  
  @abstractmethod
  def __init__(self, lc):
    pass

  @abstractmethod
  def get_list_of_files_to_process(self, passed_file_type_to_process):
    pass

  @abstractmethod
  def get_file_columns_and_schema_dict(self, passed_file_type_to_process):
    pass
