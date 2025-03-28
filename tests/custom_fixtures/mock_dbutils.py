from unitest.mock import MagicMock

from .mock_file_info import FileInfoMock

class DbutilsMock:
  fs = MagicMock()

  def __init__(self, fs_ls_list: list = None)
    if fs_ls_list:
      self.file_info = list[FileInfoMock] = []
      self.fs._ls_list = fs_ls_list
      self.fs.ls = self._dbutils_fs_ls

  def _dbutils_fs_ls(self, path):
    # path parameter is ignored and mock response returned
    file_info = []
    for filename in self.fs._ls_list:
      now_file_info = FileInfoMock(filename)
      file_info.append(now_file_info)
    return file_info


class DbutilsMockFailFNF:
  fs = MagicMock()

  def __init__(self, fs_ls_list: list = None):
    self.fs.ls = self._dbutils_fs_ls

  def _dbutils_fs_ls(self, path):
    # path parameter is ignored and mock response returned
    raise FileNotFoundError(
      "simulated error MockFileNotFoundError from DBUtils - File not found"
    )


class DbutilsMockFailF:
  fs = MagicMock()

  def __init__(self, fs_ls_list: list = None):
    self.fs.ls = self._dbutils_fs_ls

  def _dbutils_fs_ls(self, path):
    # path parameter is ignored and mock response returned
    raise Exception(
      "simulated error MockDbutilsE"
    )
