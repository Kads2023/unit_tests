import importlib

from .base_file_categories import BaseFileCategories


class FileCategoriesFactory:

    def get_file_category_object(self, passed_source_system_type) -> BBaseFileCategories:
        source_system_type = str(passed_source_system_type).lower().strip()
        class_file_name = f"{source_system_type}_file_categories"
        class_name = f"{source_system_type.capitalize()}FileCategories"
        class_module = importlib.import_module(f"pre_pre.file_categories.{class_file_name}")
        class_ref = getattr(class_module, class_name, None)
        file_category_obj: BaseFileCategories = class_ref()
        return file_category_obj
