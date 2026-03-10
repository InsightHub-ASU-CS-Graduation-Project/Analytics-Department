import pandas as pd
from get_data import *
from cleaning import *

def run_pipeline():
    #------------- fetch data (Temporarily Disabled)
    # fetcher = DataFetcher()
    # fetcher.fetch_and_save_data()

    #------------- cleaning
    search_dataframe = SearchFile()

    search_dataframe.drop_CLASS_columns(inplace = True)
    search_dataframe.drop(columns = 'adref', inplace = True)

    search_dataframe.rename_all_DOTS(inplace = True)
    search_dataframe.rename_by_substring("display_name", "name", inplace = True)

    search_dataframe.reorder_columns(
        [
            'id',
            'title',
            'description',
            'category_label',
            'category_tag',
            'company_name',
            'created',
            'contract_type',
            'contract_time',
            'location_name',
            'location_area',
            'salary_min',
            'salary_max',
            'latitude',
            'longitude'
        ],
        inplace = True
    )

    sf = search_dataframe.drop_duplicates()

if __name__ == "__main__":
    run_pipeline()