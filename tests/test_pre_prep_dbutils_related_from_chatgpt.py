# conda create --name <name> python=3.11.0 --no-default-packages
# create the folders -- D:\<folder_name>\<env_name>
# conda create --prefix D:\<folder_name>\<env_name> python=3.11.0 --no-default-packages
# conda install pyspark==3.5.4
# conda install azure-identity==1.19.0
# conda install axure-storage-file-datalate==12.17.0

# pytest D:\<folder_name>\test_pre_prep_azure_related.py

# To test the PreProcessingUtils class with pytest and monkeypatch, we need to cover the following areas:

# Constructor __init__: Test the behavior of the constructor, particularly when dbutils is passed and when it is not passed, and the fallback to DBUtils using the active Spark session.

# Method get_list_of_directories: Test the method's behavior with different inputs and check how it interacts with dbutils.fs.ls and handles exceptions like FileNotFoundError and other general exceptions.

# Method overwrite_file: Test the method with different file names and contents and check its interaction with dbutils.fs.rm and dbutils.fs.put.

# Test Cases for PreProcessingUtils with monkeypatch
# We will use monkeypatch to mock dependencies like DBUtils and FileCategoriesFactory and to simulate different conditions.


import pytest
from unittest.mock import MagicMock
from pyspark.sql import SparkSession
from your_module import PreProcessingUtils  # Replace with actual import

# Mocking DBUtils and its methods
class MockDBUtils:
    def __init__(self, spark):
        self.fs = MagicMock()  # Mocking the fs module

    class fs:
        @staticmethod
        def ls(location):
            # Simulate returning directory listings
            return [MagicMock(name="dir1", isDir=MagicMock(return_value=True)),
                    MagicMock(name="file1.txt", isDir=MagicMock(return_value=False))]

        @staticmethod
        def rm(file_name):
            pass  # Simulate the remove method

        @staticmethod
        def put(file_name, content):
            pass  # Simulate the put method


# Mocking FileCategoriesFactory and BaseFileCategories
class MockFileCategoriesFactory:
    def __init__(self, lc):
        self.lc = lc
    
    def get_file_category_object(self, source_system_type):
        # Return a mock file category object
        return MagicMock()


# Test constructor initialization
def test_init_with_dbutils(monkeypatch):
    # Monkeypatch the FileCategoriesFactory and DBUtils
    monkeypatch.setattr('your_module.FileCategoriesFactory', MockFileCategoriesFactory)
    
    # Mock DBUtils instance
    mock_dbutils = MockDBUtils(spark=None)
    
    # Initialize PreProcessingUtils with dbutils
    utils = PreProcessingUtils(lc="test_lc", source_system_type="test_system", file_type_to_process="test_type", dbutils=mock_dbutils)
    
    # Assert that dbutils is set correctly
    assert utils.dbutils == mock_dbutils


def test_init_without_dbutils(monkeypatch):
    # Monkeypatch the FileCategoriesFactory and DBUtils
    monkeypatch.setattr('your_module.FileCategoriesFactory', MockFileCategoriesFactory)

    # Mock SparkSession and DBUtils (simulate it being created automatically)
    mock_spark = MagicMock(spec=SparkSession)
    monkeypatch.setattr('pyspark.sql.SparkSession.getActiveSession', lambda: mock_spark)
    
    # Monkeypatch the import of DBUtils
    monkeypatch.setattr('pyspark.dbutils.DBUtils', MockDBUtils)
    
    # Initialize PreProcessingUtils without passing dbutils
    utils = PreProcessingUtils(lc="test_lc", source_system_type="test_system", file_type_to_process="test_type")
    
    # Assert that dbutils was initialized correctly
    assert isinstance(utils.dbutils, MockDBUtils)


# Test get_list_of_directories method
def test_get_list_of_directories(monkeypatch):
    # Monkeypatch DBUtils and FileCategoriesFactory
    monkeypatch.setattr('your_module.FileCategoriesFactory', MockFileCategoriesFactory)
    
    # Mock dbutils
    mock_dbutils = MockDBUtils(spark=None)
    utils = PreProcessingUtils(lc="test_lc", source_system_type="test_system", file_type_to_process="test_type", dbutils=mock_dbutils)
    
    # Test valid directory listing
    dirs = utils.get_list_of_directories("some/path")
    assert dirs == ["dir1"]  # Should only return directories

    # Test exception when passed_file_location is not a string
    with pytest.raises(TypeError):
        utils.get_list_of_directories(123)

    # Simulate FileNotFoundError
    mock_dbutils.fs.ls = MagicMock(side_effect=FileNotFoundError("File not found"))
    dirs = utils.get_list_of_directories("some/path")
    assert dirs is None  # The exception is caught, so it should return None

    # Simulate a general exception
    mock_dbutils.fs.ls = MagicMock(side_effect=Exception("General error"))
    dirs = utils.get_list_of_directories("some/path")
    assert dirs is None  # The exception is caught, so it should return None


# Test overwrite_file method
def test_overwrite_file(monkeypatch):
    # Monkeypatch DBUtils and FileCategoriesFactory
    monkeypatch.setattr('your_module.FileCategoriesFactory', MockFileCategoriesFactory)
    
    # Mock dbutils
    mock_dbutils = MockDBUtils(spark=None)
    utils = PreProcessingUtils(lc="test_lc", source_system_type="test_system", file_type_to_process="test_type", dbutils=mock_dbutils)

    # Test valid input
    utils.overwrite_file("test_file.txt", "some content")
    mock_dbutils.fs.rm.assert_called_once_with("test_file.txt")
    mock_dbutils.fs.put.assert_called_once_with("test_file.txt", "some content")

    # Test exception when passed_file_name is not a string
    with pytest.raises(TypeError):
        utils.overwrite_file(123, "content")

    # Test exception when passed_file_content is not a string
    with pytest.raises(TypeError):
        utils.overwrite_file("file.txt", 123)

    # Simulate a general exception in rm method
    mock_dbutils.fs.rm = MagicMock(side_effect=Exception("Error deleting file"))
    utils.overwrite_file("test_file.txt", "content")
    # Ensure that the file put method is still called
    mock_dbutils.fs.put.assert_called_once_with("test_file.txt", "content")


# Test when a method raises an exception in the PreProcessingUtils class
def test_get_list_of_directories_exception_handling(monkeypatch):
    # Mock FileCategoriesFactory and DBUtils
    monkeypatch.setattr('your_module.FileCategoriesFactory', MockFileCategoriesFactory)
    
    # Mock DBUtils
    mock_dbutils = MockDBUtils(spark=None)
    utils = PreProcessingUtils(lc="test_lc", source_system_type="test_system", file_type_to_process="test_type", dbutils=mock_dbutils)

    # Simulate an exception in fs.ls to test error handling
    mock_dbutils.fs.ls = MagicMock(side_effect=Exception("Some unexpected error"))
    dirs = utils.get_list_of_directories("some/path")
    
    # Check that the method handles the exception without crashing
    assert dirs is None  # The exception is caught and the method should return None

# Explanation of the Test Cases:
# Test Constructor Initialization:

# test_init_with_dbutils: Tests the constructor when dbutils is passed as an argument. We mock MockDBUtils and check if dbutils is assigned correctly.

# test_init_without_dbutils: Tests the constructor when dbutils is not passed. The method should automatically initialize dbutils using SparkSession.getActiveSession().

# Test get_list_of_directories Method:

# test_get_list_of_directories: Tests the behavior of the get_list_of_directories method:

# Valid input: Returns directories only.

# TypeError: If passed_file_location is not a string, it raises a TypeError.

# FileNotFoundError: Simulates FileNotFoundError to check error handling.

# General Exception: Simulates a generic exception to check error handling.

# Test overwrite_file Method:

# test_overwrite_file: Tests the behavior of the overwrite_file method:

# Valid input: Ensures that fs.rm and fs.put are called correctly.

# TypeError: Checks if TypeError is raised when the file name or content is not a string.

# Simulate errors in fs.rm: Tests that errors in the file removal process are handled properly.

# Test Exception Handling:

# test_get_list_of_directories_exception_handling: Tests the get_list_of_directories method when an exception is raised (e.g., an unexpected error). The method should handle the error gracefully without crashing.

# How to Run:
# Save the test code in a file (e.g., test_preprocessing_utils.py).

# Run the tests using pytest:

# bash
# Copy
# pytest test_preprocessing_utils.py
# These tests cover the core functionality of the PreProcessingUtils class, including constructor behavior, error handling, and interactions with dbutils.fs.ls, dbutils.fs.rm, and dbutils.fs.put.

# Test overwrite_file:

# test_overwrite_file: Tests the overwrite_file method by checking that the rm and put methods of dbutils.fs are called correctly. It also checks that the method raises TypeError when the arguments are not strings.
