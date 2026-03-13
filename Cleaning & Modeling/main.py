import pandas as pd
from get_data import *
from cleaning import *

def run_pipeline():
    # ------------- fetch data (Temporarily Disabled)
    # fetcher = DataFetcher()
    # fetcher.fetch_and_save_data(total_target_jobs = 55555)
    pass

    # ------------- cleaning
search_dataframe = SearchFile()

search_dataframe.drop_CLASS_columns(inplace = True)
search_dataframe.drop(columns = 'adref', inplace = True)
search_dataframe.drop(columns = 'salary_is_predicted', inplace = True)

search_dataframe.rename_all_DOTS(inplace = True)
search_dataframe.rename_by_substring("display_name", "name", inplace = True)

search_dataframe.reorder_columns(
    'id',
    'title',
    'category_label',
    'category_tag',
    'company_name',
    'created',
    'contract_type',
    'contract_time',
    'salary_min',
    'salary_max',
    'location_name',
    'location_area',
    'latitude',
    'longitude',
    inplace = True
)

search_dataframe['created'] = pd.to_datetime(search_dataframe['created'])

search_dataframe.extract_keywords(
    'title',
    inplace = True,
    remove_extracted = True,
    seniority_level = ['intern', 'trainee', 'principal', 'senior', 'mid-level', 'junior'],
    work_setup = ['remote', 'hybrid', 'onsite']
    )
search_dataframe.extract_keywords(
    'title',
    inplace = True,
    remove_extracted = False,
    seniority_level = ['chief', 'director', 'head', 'lead', 'manager']
)

search_dataframe.split_list_objects(
    source_col = 'location_area',
    remove_source_col = True,
    inplace = True,
    country = 0,
    state_or_gov = 1
)

search_dataframe.impute_by_language('category_tag', 'category_label', target_lang = 'en', inplace = True)

search_dataframe.loc[search_dataframe['category_tag'] == 'hr-jobs', 'category_label'] = 'HR Jobs'
search_dataframe.loc[
    search_dataframe['category_tag'] == 'pr-advertising-marketing-jobs',
    'category_label'
    ] = 'PR, Advertising & Marketing Jobs'

# if __name__ == "__main__":
#     run_pipeline()