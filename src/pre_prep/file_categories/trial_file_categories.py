from .base_file_categories import BaseFileCategories


class TrialFileCategories(BaseFileCategories):
    dict_of_file_type_wise_list_of_files_to_process = {'trial_1': ['trial.csv']}
    dict_of_file_type_wise_file_columns_and_schema = {
      'trial_1': {
        "data": {
          "header": "Type,Term",
          "string_to_compare": '"Type","Term"'
        }
        "data_currency_basis": {
          "header": "Basis_Type,Basis_Term,Basis_Offer",
          "string_to_compare": '"Type","Term","Basis Offer"'
        }
      }
    }
  
    def __init__(self, lc):
        self.lc = lc
  
    def get_list_of_files_to_process(self, passed_file_type_to_process):
        return self.dict_of_file_type_wise_list_of_files_to_process[passed_file_type_to_process]
  
    def get_file_columns_and_schema_dict(self, passed_file_type_to_process):
        return self.dict_of_file_type_wise_file_columns_and_schema[passed_file_type_to_process]
