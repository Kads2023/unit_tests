# pytest all possible test cases with monkeypatch for the below code

from pyspark.sql import SparkSession

from .file_categories.file_category_factory import FileCategoriesFactory
from .file_categories.base_file_categories import BaseFileCategories


class PreProcessingUtils:
    def __init__(self, lc, source_system_type, file_type_to_process, dbutils=None):
        self.lc = lc
        self.file_category_obj: BaseFileCategories = FileCategoriesFactory(
          lc).get_file_category_object(source_system_type)
        self.file_type_to_process = file_type_to_process

        if dbutils is not None:
          self.dbutils = dbutils
        else:
          spark = SparkSession.getActiveSession()
          try:
            from pyspark.dbutils import DBUtils

            self.dbutils = DBUtils(spark)
          except ModuleNotFoundError:
            self.dbutils =  None


    def get_list_of_directories(self, passed_file_location):
      if type(passed_file_location) is not str:
        raise TypeError("passed_file_location must be string")

      dir_list = []
      list_of_folders = self.dbutils.fs.ls(passed_file_location)
      for each_listing in list_of_folders:
        each_name = each_listing.name
        if each_name.endswith("/"):
          dir_list.append(each_name)
      return dir_list


    def overwrite_file(self, passed_file_name, passed_file_content):
      if type(passed_file_name) is not str:
        raise TypeError("passed_file_name must be string")
      if type(passed_file_content) is not str:
        raise TypeError("passed_file_content must be string")
        
      try:
        self.dbutils.fs.rm(passed_file_name)
      except Exception as e:
        print("Exception occured while deleting the file, exception --> {e}")
      self.sbutils.fs.put(passed_file_name, passed_file_content)
  
      
    
