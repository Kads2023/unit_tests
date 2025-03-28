import pytest
from unittest.mock import MagicMock
import importlib
from pre_prep.file_categories.file_category_factory import FileCategoriesFactory

from ..tests.custom_fixtures.mock_logger import MockLogger


@pytest.fixture(name="lc", autouse=True)
def fixture_logger():
  return MockLogger()


@pytest.fixture
def mock_import_module():
  mock_trial_module = MagicMock()
  monkeypath.setattr(importlib, name:"import_module", lambda name: mock_trial_module)

  mock_trial_class = MagicMock()
  mock_trial_class.BondFileCategories = mock_trial_class

  return mock_trial_class


def test_get_file_category_object(mock_import_module, lc):
  factory = FileCategoriesFactory(lc)

  file_category_obj = factory.get_file_category_object("bond")

  mock_import_module.assert_called()
  assert file_category_obj is file_category_obj(lc)
  assert isinstance(file_category_obj, MagicMock)




