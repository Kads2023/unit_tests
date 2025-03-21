from collections import namedtuple
from unitest.mock import MagicMock


class DbutilsMock;
  fs = MagicMock()

  def __init__(self, fs_ls_list: list = None)
    if fs_ls_list:
      self.file_info = namedtuple(
        typename:"FileInfo", field_names:["path", "name", "size", "modificationTime"]
      )
      self.fs._ls_list = fs_ls_list
      self.fs.ls = self._dbutils_fs_ls

  def _dbutils_fs_ls(self, path):
    # path parameter is ignored and mock response returned
    file_info = []
    for filename in self.fs._ls_list:
      file_info.append(
        self.file_info(
          "abfss://volume@storage_account_name.dfs.core.windows.net/volume/"
          + filename,
          filename,
          100000,
          1739381597,
        )
      )
    return file_info
      
