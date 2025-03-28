import importlib
from .base_file_categories import BaseFileCategories


class FileCategoriesFactory:

    def __init__(self, lc):
        self.lc = lc
  
    def get_file_category_object(self, passed_source_system_type) -> BBaseFileCategories:
        this_module = f"[{type(self).__name__}.get_file_category_object()] -"
        try:
            source_system_type = str(passed_source_system_type).lower().strip()
            class_file_name = f"{source_system_type}_file_categories"
            class_name = f"{source_system_type.capitalize()}FileCategories"
            class_module = importlib.import_module(f"pre_pre.file_categories.{class_file_name}")
            class_ref = getattr(class_module, class_name, None)
            file_category_obj: BaseFileCategories = class_ref(self.lc)
            return file_category_obj
        except ModuleNotFoundError as ex:
            error_msg = (f"{this_module} UNKNOWN: "
                         f"source_system_type -- {passed_source_system_type}, "
                         f"Implementation available for "
                         f"banking / bond / risk")
            print error_msg
            raise
        except Exception as e:
            error_msg = (f"{this_module} UNKNOWN: "
                         f"source_system_type -- {passed_source_system_type}, "
                         f"({e})")
            print error_msg
            raise
            
