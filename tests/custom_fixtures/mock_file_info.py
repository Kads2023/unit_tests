from unittest.mock import MagicMock


class FileInfoMock:
  mock_fileinfo = MagicMock()

  def __init__(self, file_name: str):
    self.name = file_name
    if file_name.endswith("/"):
      self.is_dir = True
    else:
      self.is_dir = False
    self.size = 100000
    self.path = f"abfss://volume@storage_account_name.dfs.core.windows.net/volume/{file_name}"
    self.modificationTime = 1739381597

  def isDir(self):
    return self.is_dir
