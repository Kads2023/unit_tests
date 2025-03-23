# conda create --name <name> python=3.11.0 --no-default-packages
# create the folders -- D:\<folder_name>\<env_name>
# conda create --prefix D:\<folder_name>\<env_name> python=3.11.0 --no-default-packages
# conda install pyspark==3.5.4
# conda install azure-identity==1.19.0
# conda install axure-storage-file-datalate==12.17.0

# pytest D:\<folder_name>\test_pre_prep_azure_related.py


# To write tests for the PreProcessingUtils class using pytest and monkeypatch, we need to focus on testing the following functionalities:

# Constructor __init__ - We need to check the behavior of the constructor when the dbutils argument is provided and when it is not. This will allow us to test the logic where the dbutils is assigned to a DBUtils instance or is set to None.

# Method get_list_of_directories - This method interacts with dbutils.fs.ls() to list directories and checks the type of the input to ensure it’s a string. We will need to mock the dbutils.fs.ls function.

# Method overwrite_file - This method interacts with dbutils.fs.rm and dbutils.fs.put. We will need to mock these functions and test scenarios such as successful deletion and file writing.

# Let's write the pytest test cases with monkeypatch for these methods.


import pytest
from unittest.mock import MagicMock
from your_module import PreProcessingUtils  # Replace with actual import
from pyspark.sql import SparkSession


# Mocking the FileCategoriesFactory and BaseFileCategories
class MockFileCategoriesFactory:
    def __init__(self, lc):
        pass

    def get_file_category_object(self, source_system_type):
        return MagicMock()  # Mocking the file category object


# Mocking the dbutils
class MockDBUtils:
    def __init__(self, spark):
        self.fs = MagicMock()  # Mocking the fs module

    class fs:
        @staticmethod
        def ls(location):
            return []  # Return an empty list for simplicity

        @staticmethod
        def rm(file_name):
            pass  # Mock rm functionality

        @staticmethod
        def put(file_name, content):
            pass  # Mock put functionality


@pytest.fixture
def mock_spark():
    return SparkSession.builder.master("local").appName("Test").getOrCreate()


# Test constructor initialization
def test_init_with_dbutils(mock_spark, monkeypatch):
    # Monkeypatch DBUtils and FileCategoriesFactory
    monkeypatch.setattr('your_module.FileCategoriesFactory', MockFileCategoriesFactory)

    mock_dbutils = MockDBUtils(mock_spark)
    utils = PreProcessingUtils(lc="test_lc", source_system_type="test_system", file_type_to_process="test_type", dbutils=mock_dbutils)
    
    assert utils.dbutils == mock_dbutils
    assert isinstance(utils.file_category_obj, MagicMock)  # Check that the file category object is mocked


def test_init_without_dbutils(mock_spark, monkeypatch):
    # Monkeypatch DBUtils and FileCategoriesFactory
    monkeypatch.setattr('your_module.FileCategoriesFactory', MockFileCategoriesFactory)
    
    # Monkeypatch the DBUtils to simulate no DBUtils being passed
    monkeypatch.setattr('pyspark.sql.SparkSession.getActiveSession', lambda: mock_spark)
    monkeypatch.setattr('pyspark.dbutils.DBUtils', MockDBUtils)

    utils = PreProcessingUtils(lc="test_lc", source_system_type="test_system", file_type_to_process="test_type")
    
    # Check that DBUtils was created
    assert isinstance(utils.dbutils, MockDBUtils)


# Test get_list_of_directories
def test_get_list_of_directories(mock_spark, monkeypatch):
    # Setup mock dbutils
    mock_dbutils = MockDBUtils(mock_spark)
    utils = PreProcessingUtils(lc="test_lc", source_system_type="test_system", file_type_to_process="test_type", dbutils=mock_dbutils)

    # Mock dbutils.fs.ls to return a list with directories
    mock_dbutils.fs.ls = MagicMock(return_value=[MagicMock(name="dir1", endswith=MagicMock(return_value=True)),
                                                 MagicMock(name="file1.txt", endswith=MagicMock(return_value=False))])

    dir_list = utils.get_list_of_directories("some/path")
    
    assert dir_list == ['dir1/']  # Check that only directories are returned

    # Test exception when passed_file_location is not a string
    with pytest.raises(TypeError):
        utils.get_list_of_directories(123)


# Test overwrite_file method
def test_overwrite_file(mock_spark, monkeypatch):
    # Setup mock dbutils
    mock_dbutils = MockDBUtils(mock_spark)
    utils = PreProcessingUtils(lc="test_lc", source_system_type="test_system", file_type_to_process="test_type", dbutils=mock_dbutils)

    # Monkeypatch dbutils.fs.rm and dbutils.fs.put to mock behavior
    mock_dbutils.fs.rm = MagicMock()
    mock_dbutils.fs.put = MagicMock()

    utils.overwrite_file("test_file.txt", "some content")

    # Check that rm and put were called with the correct arguments
    mock_dbutils.fs.rm.assert_called_once_with("test_file.txt")
    mock_dbutils.fs.put.assert_called_once_with("test_file.txt", "some content")

    # Test exception when passed_file_name is not a string
    with pytest.raises(TypeError):
        utils.overwrite_file(123, "content")

    # Test exception when passed_file_content is not a string
    with pytest.raises(TypeError):
        utils.overwrite_file("file.txt", 123)
# Explanation of Tests:
# Test Constructor Initialization:

# test_init_with_dbutils: Tests if the dbutils argument is properly passed to the PreProcessingUtils constructor. The test uses monkeypatch to replace the FileCategoriesFactory class and checks if dbutils is assigned correctly.

# test_init_without_dbutils: Tests the case when dbutils is not provided and the constructor falls back on using the active Spark session to create dbutils. It ensures the correct behavior by mocking the Spark session.

# Test get_list_of_directories:

# test_get_list_of_directories: Tests the method get_list_of_directories for a valid string path. The dbutils.fs.ls method is mocked to return a mixture of directories and files. It checks that only directories are returned.

# The test also checks that a TypeError is raised when the input to the method is not a string.

# Test overwrite_file:

# test_overwrite_file: Tests the overwrite_file method by checking that the rm and put methods of dbutils.fs are called correctly. It also checks that the method raises TypeError when the arguments are not strings.
