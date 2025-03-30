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


#
# from abc import abstractmethod
#
#
# class BaseFileCategories:
#   """
#   An abstract base class for handling different file categories.
#
#   This class defines the structure for retrieving file lists and schemas
#   based on a specific file type.
#   """
#
#   @abstractmethod
#   def __init__(self, lc):
#     """
#     Initializes the BaseFileCategories instance.
#
#     Parameters:
#     lc: Configuration or context parameter required for initialization.
#     """
#     pass
#
#   @abstractmethod
#   def get_list_of_files_to_process(self, passed_file_type_to_process):
#     """
#     Retrieves a list of files to be processed for a given file type.
#
#     Parameters:
#     passed_file_type_to_process (str): The type of file to process.
#
#     Returns:
#     list: A list of file names or paths that need processing.
#     """
#     pass
#
#   @abstractmethod
#   def get_file_columns_and_schema_dict(self, passed_file_type_to_process):
#     """
#     Retrieves the columns and schema dictionary for a given file type.
#
#     Parameters:
#     passed_file_type_to_process (str): The type of file whose schema is needed.
#
#     Returns:
#     dict: A dictionary mapping column names to their data types.
#     """
#     pass
